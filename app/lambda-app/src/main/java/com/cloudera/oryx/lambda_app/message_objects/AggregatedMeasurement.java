package com.cloudera.oryx.lambda_app.message_objects;

import java.util.Date;

/**
 * Created by aclima on 11/01/2017.
 */

public class AggregatedMeasurement extends Measurement {

    private Date sampleStartTime;
    private Date sampleEndTime;

    public AggregatedMeasurement(Integer id, String name, Date sampleStartTime, Date sampleEndTime) {
        super(id, name);
        this.sampleStartTime = sampleStartTime;
        this.sampleEndTime = sampleEndTime;
    }

    public Date getSampleStartTime() {
        return sampleStartTime;
    }

    public Date getSampleEndTime() {
        return sampleEndTime;
    }

}
