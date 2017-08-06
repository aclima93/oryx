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

package com.cloudera.oryx.lambda_app.serving;

import com.cloudera.oryx.api.serving.AbstractServingModelManager;
import com.cloudera.oryx.api.serving.ServingModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads models and updates produced by the Batch Layer and Speed Layer. Models are maps, encoded as JSON
 * strings, mapping DeviceMessages to count of distinct other DeviceMessages that appear with that DeviceMessage
 * in an input line.
 * Updates are "DeviceMessage,count" pairs representing new counts for a DeviceMessage.
 * This class manages and exposes the mapping to the Serving Layer applications.
 */
public final class ExampleServingModelManager extends AbstractServingModelManager<String> {

  private final Map<String,Integer> distinctDeviceMessages = new HashMap<>();

  public ExampleServingModelManager(Config config) {
    super(config);
  }

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "MODEL":
        @SuppressWarnings("unchecked")
        Map<String,Integer> model = (Map<String,Integer>) new ObjectMapper().readValue(message, Map.class);
        synchronized (distinctDeviceMessages) {
          distinctDeviceMessages.clear();
          model.forEach(distinctDeviceMessages::put);
        }
        break;
      case "UP":
        String[] measurementCount = message.split(",");
        String deviceMessage = measurementCount[0];
        String count = measurementCount[1];
        synchronized (distinctDeviceMessages) {
          distinctDeviceMessages.put(deviceMessage, Integer.valueOf(count));
        }
        break;
      default:
        throw new IllegalArgumentException("Bad key " + key);
    }
  }

  @Override
  public ServingModel getModel() {
    return new ExampleServingModel(distinctDeviceMessages);
  }

}