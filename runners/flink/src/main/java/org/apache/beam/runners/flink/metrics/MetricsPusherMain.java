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
package org.apache.beam.runners.flink.metrics;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

/** main class for spark test. */
public class MetricsPusherMain {

  public static void main(String[] args) throws InterruptedException {
    LoggerFactory.getLogger(MetricsPusherMain.class)
        .error("******* RUN  MetricsPusherMain **********");
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            // Use maxReadTime to force unbounded mode.
            GenerateSequence.from(0).to(100000L).withMaxReadTime(Duration.standardDays(1)))
        .apply(ParDo.of(new CountingDoFn()));
    pipeline.run();
    // give pipeline some time to push
    Thread.sleep((options.getMetricsPushPeriod() + 1) * 10000L);
  }

  private static class CountingDoFn extends DoFn<Long, Long> {
    private final Counter counter = Metrics.counter(MetricsPusherMain.class, "counter");

    @ProcessElement
    public void processElement(ProcessContext context) {
      counter.inc();
      context.output(context.element());
    }
  }
}
