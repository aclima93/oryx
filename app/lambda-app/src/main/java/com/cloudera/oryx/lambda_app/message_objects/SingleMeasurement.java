package com.cloudera.oryx.lambda_app.message_objects;

import java.util.Date;

/**
 * Created by aclima on 11/01/2017.
 */

public class SingleMeasurement extends Measurement {

    private String value;
    private String unitsOfMeasurement;
    private Date timestamp;

    public SingleMeasurement(Integer id, String name, String value, String unitsOfMeasurement, Date timestamp) {
        super(id, name);
        this.value = value;
        this.unitsOfMeasurement = unitsOfMeasurement;
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public String getUnitsOfMeasurement() {
        return unitsOfMeasurement;
    }

    public Date getTimestamp() {
        return timestamp;
    }

}
