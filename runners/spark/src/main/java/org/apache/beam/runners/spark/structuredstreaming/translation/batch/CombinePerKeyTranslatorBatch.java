/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.KVHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SideInputBroadcast;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KeyValueGroupedDataset;
import scala.Tuple2;

class CombinePerKeyTranslatorBatch<K, V, AccumT, OutputT>
    implements TransformTranslator<
        PTransform<PCollection<KV<K, V>>, PCollection<KV<K, OutputT>>>> {

  @Override
  public void translateTransform(
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, OutputT>>> transform,
      TranslationContext context) {

    Combine.PerKey combineTransform = (Combine.PerKey) transform;
    @SuppressWarnings("unchecked")
    final PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) context.getInput();
    @SuppressWarnings("unchecked")
    final PCollection<KV<K, OutputT>> output = (PCollection<KV<K, OutputT>>) context.getOutput();
    @SuppressWarnings("unchecked")
    final CombineWithContext.CombineFnWithContext<V, AccumT, OutputT> combineFn =
        (CombineWithContext.CombineFnWithContext<V, AccumT, OutputT>)
            CombineFnUtil.toFnWithContext(combineTransform.getFn());

    WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

    Dataset<WindowedValue<KV<K, V>>> inputDataset = context.getDataset(input);
    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = inputCoder.getKeyCoder();
    KvCoder<K, OutputT> outputKVCoder = (KvCoder<K, OutputT>) output.getCoder();
    Coder<OutputT> outputCoder = outputKVCoder.getValueCoder();

    List<PCollectionView<?>> sideInputs = combineTransform.getSideInputs();
    Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputStrategies = new HashMap<>();
    for (PCollectionView<?> sideInput : sideInputs) {
      sideInputStrategies.put(sideInput, sideInput.getPCollection().getWindowingStrategy());
    }

    SideInputBroadcast broadcastStateData =
        SideInputBroadcast.createBroadcastSideInputs(sideInputs, context);

    Coder<AccumT> accumulatorCoder;
    try {
      accumulatorCoder =
          combineFn.getAccumulatorCoder(
              input.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }
    final SparkCombineFn<KV<K, V>, V, AccumT, OutputT> sparkCombineFn =
        SparkCombineFn.keyed(
            combineFn,
            context.getSerializableOptions(),
            sideInputStrategies,
            broadcastStateData,
            windowingStrategy, accumulatorCoder, outputCoder);

    KeyValueGroupedDataset<K, WindowedValue<KV<K, V>>> groupedDataset =
        inputDataset.groupByKey(KVHelpers.extractKey(), EncoderHelpers.fromBeamCoder(keyCoder));
    Dataset<Tuple2<K, Iterable<WindowedValue<OutputT>>>> combinedDataset = groupedDataset.<Iterable<WindowedValue<OutputT>>>agg(
        sparkCombineFn.toColumn());

    // expand the list into separate elements and put the key back into the elements
    WindowedValue.WindowedValueCoder<KV<K, OutputT>> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(
            outputKVCoder, input.getWindowingStrategy().getWindowFn().windowCoder());
    Dataset<WindowedValue<KV<K, OutputT>>> outputDataset =
        combinedDataset.flatMap(
            (FlatMapFunction<
                    Tuple2<K, Iterable<WindowedValue<OutputT>>>, WindowedValue<KV<K, OutputT>>>)
                tuple2 -> {
                  K key = tuple2._1();
                  Iterable<WindowedValue<OutputT>> windowedValues = tuple2._2();
                  List<WindowedValue<KV<K, OutputT>>> result = new ArrayList<>();
                  for (WindowedValue<OutputT> windowedValue : windowedValues) {
                    KV<K, OutputT> kv = KV.of(key, windowedValue.getValue());
                    result.add(
                        WindowedValue.of(
                            kv,
                            windowedValue.getTimestamp(),
                            windowedValue.getWindows(),
                            windowedValue.getPane()));
                  }
                  return result.iterator();
                },
            EncoderHelpers.fromBeamCoder(wvCoder));
    context.putDataset(output, outputDataset);
  }
}
