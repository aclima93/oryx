package com.cloudera.oryx.lambda_app.message_objects;

import java.util.ArrayList;

/**
 * Created by aclima on 22/07/2017.
 */

public class DeviceMessage {

    private String deviceId;
    private String payload;
    private String subtopic;

    // get measurements from the payload
    private ArrayList<Measurement> measurements;

    DeviceMessage(String deviceId, String payload, String subtopic) {
        this.deviceId = deviceId;
        this.subtopic = subtopic;
        this.payload = payload;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getPayload() {
        return payload;
    }

    public String getSubtopic() {
        return subtopic;
    }
}
