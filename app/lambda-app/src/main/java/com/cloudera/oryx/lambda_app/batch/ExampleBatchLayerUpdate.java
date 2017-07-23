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

package com.cloudera.oryx.lambda_app.batch;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.batch.BatchLayerUpdate;
import com.cloudera.oryx.lambda_app.message_objects.DeviceMessage;
import com.cloudera.oryx.lambda_app.message_objects.EventfulAggregatedMeasurement;
import com.cloudera.oryx.lambda_app.message_objects.EventfulMeasurement;
import com.cloudera.oryx.lambda_app.message_objects.Measurement;
import com.cloudera.oryx.lambda_app.message_objects.OneTimeMeasurement;
import com.cloudera.oryx.lambda_app.message_objects.PeriodicAggregatedMeasurement;
import com.cloudera.oryx.lambda_app.message_objects.PeriodicMeasurement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Input keys are ignored. Values are treated as lines of space-separated text. The job
 * counts, for each word, the number of distinct other words that co-occur in some line
 * of text in the input. These are written as a "MODEL" update, where the word-count mapping
 * is written as a JSON string.
 */
public final class ExampleBatchLayerUpdate implements BatchLayerUpdate<String,String,String> {

  @Override
  public void runUpdate(JavaSparkContext sparkContext,
                        long timestamp,
                        JavaPairRDD<String,String> newData,
                        JavaPairRDD<String,String> pastData,
                        String modelDirString,
                        TopicProducer<String,String> modelUpdateTopic) throws IOException {

    // new data is joined with past data, if there is any
    JavaPairRDD<String,String> allData = pastData == null ? newData : newData.union(pastData);
    String modelString;
    try {
      modelString = new ObjectMapper().writeValueAsString(countDistinctMeasurements(allData));
    } catch (JsonProcessingException jpe) {
      throw new IOException(jpe);
    }
    modelUpdateTopic.send("MODEL", modelString);
  }

  public static Map<Measurement, Integer> countDistinctMeasurements(JavaPairRDD<String,String> data) {
    return data.values().flatMapToPair(line -> {

      // get the message object from the JSON string
      final Gson gson = new Gson();
      DeviceMessage deviceMessage = gson.fromJson(line, DeviceMessage.class);
      Set<Measurement> distinctMeasurements;
      Type listType;

      // the payload itself is also a JSON string representing a list of measurements for a particular class/topic
      switch (deviceMessage.getSubtopic()) {

        case "PeriodicMeasurement":
          listType = new TypeToken<ArrayList<PeriodicMeasurement>>() {
          }.getType();
          distinctMeasurements = new HashSet<>(gson.fromJson(deviceMessage.getPayload(), listType));
          break;

        case "EventfulMeasurement":
          listType = new TypeToken<ArrayList<EventfulMeasurement>>() {
          }.getType();
          distinctMeasurements = new HashSet<>(gson.fromJson(deviceMessage.getPayload(), listType));
          break;

        case "OneTimeMeasurement":
          listType = new TypeToken<ArrayList<OneTimeMeasurement>>() {
          }.getType();
          distinctMeasurements = new HashSet<>(gson.fromJson(deviceMessage.getPayload(), listType));
          break;

        case "PeriodicAggregatedMeasurement":
          listType = new TypeToken<ArrayList<PeriodicAggregatedMeasurement>>() {
          }.getType();
          distinctMeasurements = new HashSet<>(gson.fromJson(deviceMessage.getPayload(), listType));
          break;

        case "EventfulAggregatedMeasurement":
          listType = new TypeToken<ArrayList<EventfulAggregatedMeasurement>>() {
          }.getType();
          distinctMeasurements = new HashSet<>(gson.fromJson(deviceMessage.getPayload(), listType));
          break;

        default:
          distinctMeasurements = new HashSet<>();
          break;
      }

      return distinctMeasurements.stream().flatMap(a ->
              distinctMeasurements.stream().filter(b -> !a.equals(b)).map(b -> new Tuple2<>(a, b))
      ).iterator();
    }).distinct().mapValues(a -> 1).reduceByKey((c1, c2) -> c1 + c2).collectAsMap();
  }

}