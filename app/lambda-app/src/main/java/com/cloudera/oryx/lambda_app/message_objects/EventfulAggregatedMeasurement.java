package com.cloudera.oryx.lambda_app.message_objects;

import java.util.Date;

/**
 * Created by aclima on 13/12/2016.
 */

public class EventfulAggregatedMeasurement extends AggregatedMeasurement{

    private Integer numberOfEvents;

    public EventfulAggregatedMeasurement(Integer id, String name, Date sampleStartTime, Date sampleEndTime,
                                         Integer numberOfEvents) {
        super(id, name, sampleStartTime, sampleEndTime);
        this.numberOfEvents = numberOfEvents;
    }

    public Integer getNumberOfEvents() {
        return numberOfEvents;
    }

    public void setNumberOfEvents(Integer numberOfEvents) {
        this.numberOfEvents = numberOfEvents;
    }
}
