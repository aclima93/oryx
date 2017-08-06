/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.oryx.api.serving.ServingModel;

import java.util.Map;

/**
 * {@link ServingModel} produced by {@link ExampleServingModelManager}.
 */
public final class ExampleServingModel implements ServingModel {

  private final Map<String,Integer> distinctDeviceMessages;

  ExampleServingModel(Map<String,Integer> distinctDeviceMessages) {
    this.distinctDeviceMessages = distinctDeviceMessages;
  }

  @Override
  public float getFractionLoaded() {
    return 1.0f;
  }

  public Map<String,Integer> getDistinctDeviceMessages() {
    return distinctDeviceMessages;
  }

}
