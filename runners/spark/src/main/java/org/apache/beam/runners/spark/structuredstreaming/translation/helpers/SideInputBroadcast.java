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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

/** Broadcast helper for side inputs. */
public class SideInputBroadcast implements Serializable {

  private final Map<String, Broadcast<?>> bcast = new HashMap<>();
  private final Map<String, Coder<?>> coder = new HashMap<>();

  public SideInputBroadcast() {}

  public static SideInputBroadcast createBroadcastSideInputs(
      List<PCollectionView<?>> sideInputs, TranslationContext context) {
    JavaSparkContext jsc =
        JavaSparkContext.fromSparkContext(context.getSparkSession().sparkContext());

    SideInputBroadcast sideInputBroadcast = new SideInputBroadcast();
    for (PCollectionView<?> sideInput : sideInputs) {
      Coder<? extends BoundedWindow> windowCoder =
          sideInput.getPCollection().getWindowingStrategy().getWindowFn().windowCoder();

      Coder<WindowedValue<?>> windowedValueCoder =
          (Coder<WindowedValue<?>>)
              (Coder<?>)
                  WindowedValue.getFullCoder(sideInput.getPCollection().getCoder(), windowCoder);
      Dataset<WindowedValue<?>> broadcastSet = context.getSideInputDataSet(sideInput);
      List<WindowedValue<?>> valuesList = broadcastSet.collectAsList();
      List<byte[]> codedValues = new ArrayList<>();
      for (WindowedValue<?> v : valuesList) {
        codedValues.add(CoderHelpers.toByteArray(v, windowedValueCoder));
      }

      sideInputBroadcast.add(
          sideInput.getTagInternal().getId(), jsc.broadcast(codedValues), windowedValueCoder);
    }
    return sideInputBroadcast;
  }

  public void add(String key, Broadcast<?> bcast, Coder<?> coder) {
    this.bcast.put(key, bcast);
    this.coder.put(key, coder);
  }

  public Broadcast<?> getBroadcastValue(String key) {
    return bcast.get(key);
  }

  public Coder<?> getCoder(String key) {
    return coder.get(key);
  }
}
