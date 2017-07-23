package com.cloudera.oryx.lambda_app.message_objects;

/**
 * Created by aclima on 22/07/2017.
 */

public class DeviceMessage {

    private String subtopic;
    private String deviceId;
    private String payload;

    DeviceMessage(String subtopic, String deviceId, String payload) {
        this.subtopic = subtopic;
        this.deviceId = deviceId;
        this.payload = payload;
    }

    public String getSubtopic() {
        return subtopic;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getPayload() {
        return payload;
    }
}
