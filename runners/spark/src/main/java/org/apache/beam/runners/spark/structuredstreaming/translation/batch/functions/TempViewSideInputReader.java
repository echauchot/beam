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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.RowHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** A {@link SideInputReader} for the Spark Batch Runner. */
public class TempViewSideInputReader implements SideInputReader {

  private SparkSession sparkSession;
  private Map<TupleTag<?>, WindowingStrategy<?, ?>> sideInputs;

  public TempViewSideInputReader(
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> viewToWindowingStrategy, SparkSession sparkSession) {
    for (PCollectionView<?> view : viewToWindowingStrategy.keySet()) {
      checkArgument(
          Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
              view.getViewFn().getMaterialization().getUrn()),
          "This handler is only capable of dealing with %s materializations "
              + "but was asked to handle %s for PCollectionView with tag %s.",
          Materializations.MULTIMAP_MATERIALIZATION_URN,
          view.getViewFn().getMaterialization().getUrn(),
          view.getTagInternal().getId());
    }
    sideInputs = new HashMap<>();
    for (Map.Entry<PCollectionView<?>, WindowingStrategy<?, ?>> entry : viewToWindowingStrategy.entrySet()) {
      sideInputs.put(entry.getKey().getTagInternal(), entry.getValue());
    }
    this.sparkSession = sparkSession;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    checkNotNull(view, "View passed to sideInput cannot be null");
    TupleTag<?> tag = view.getTagInternal();
    checkNotNull(sideInputs.get(tag), "Side input for " + view + " not available.");
    // TODO improve when dataset schema contains window, replace by a "where window = window"
    // initialize variables
    Coder<? extends BoundedWindow> windowCoder = sideInputs.get(tag).getWindowFn().windowCoder();
    Coder kvCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
    WindowedValue.FullWindowedValueCoder windowedValueCoder = WindowedValue.FullWindowedValueCoder
        .of(kvCoder, windowCoder);
    final BoundedWindow sideInputWindow = view.getWindowMappingFn().getSideInputWindow(window);

    //TODO test to store only dataset in the TranslationContext and use no temp view

    // get the PCollectionView created by the user
    Dataset<Row> rowDataset = sparkSession.sql(
        "SELECT * from " + getViewName(view.getTagInternal().getId()));

    Dataset<WindowedValue<KV<?, ?>>> availableSideInputs = rowDataset
        .map(RowHelpers.extractWindowedValueFromRowMapFunction(windowedValueCoder),
            EncoderHelpers.fromBeamCoder(windowedValueCoder));
    // filter it by the given window
    Dataset<WindowedValue<KV<?, ?>>> sideInputForWindow = availableSideInputs.filter(
        (FilterFunction<WindowedValue<KV<?, ?>>>) windowedValue -> windowedValue == null ?
            false :
            windowedValue.getWindows().contains(sideInputWindow));

    ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
    return viewFn.apply(InMemoryMultimapSideInputView.fromIterable(kvCoder, sideInputForWindow.map(
        (MapFunction<WindowedValue<KV<?, ?>>, KV<?, ?>>) windowedValue -> windowedValue.getValue(),
        EncoderHelpers.fromBeamCoder(kvCoder)).collectAsList()));
  }



  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  public static String getViewName(String tagId){
    return tagId.replaceAll("[$<>#:;.]", "_");
  }
}
