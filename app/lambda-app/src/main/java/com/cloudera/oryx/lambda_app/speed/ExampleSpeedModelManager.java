/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.lambda_app.speed;

import com.cloudera.oryx.api.speed.AbstractSpeedModelManager;
import com.cloudera.oryx.lambda_app.batch.ExampleBatchLayerUpdate;
import com.cloudera.oryx.lambda_app.message_objects.Measurement;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Also counts and emits counts of number of distinct words that occur with words.
 * Listens for updates from the Batch Layer, which give the current correct count at its
 * last run. Updates these counts approximately in response to the same data stream
 * that the Batch Layer sees, but assumes all words seen are new and distinct, which is only
 * approximately true. Emits updates of the form "word,count".
 */
public final class ExampleSpeedModelManager extends AbstractSpeedModelManager<String,String,String> {

  private final Map<Measurement,Integer> distinctMeasurements = new HashMap<>();

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "MODEL":
        @SuppressWarnings("unchecked")
        Map<Measurement,Integer> model = (Map<Measurement,Integer>) new ObjectMapper().readValue(message, Map.class);
        synchronized (distinctMeasurements) {
          distinctMeasurements.clear();
          model.forEach(distinctMeasurements::put);
        }
        break;
      case "UP":
        // ignore
        break;
      default:
        throw new IllegalArgumentException("Bad key " + key);
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    return ExampleBatchLayerUpdate.countDistinctMeasurements(newData).entrySet().stream().map(entry -> {
      Measurement measurement = entry.getKey();
      int count = entry.getValue();
      int newCount;
      synchronized (distinctMeasurements) {
        Integer oldCount = distinctMeasurements.get(measurement);
        newCount = oldCount == null ? count : oldCount + count;
        distinctMeasurements.put(measurement, newCount);
      }
      return measurement + "," + newCount;
    }).collect(Collectors.toList());
  }

}