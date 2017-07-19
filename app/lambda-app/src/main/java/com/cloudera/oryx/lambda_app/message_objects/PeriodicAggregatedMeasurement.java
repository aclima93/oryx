package com.cloudera.oryx.lambda_app.message_objects;

import java.util.Date;

/**
 * Created by aclima on 13/12/2016.
 */

public class PeriodicAggregatedMeasurement extends AggregatedMeasurement{

    private Double harmonicValue;
    private Double medianValue;
    private String unitsOfMeasurement;

    public PeriodicAggregatedMeasurement(Integer id, String name, Date sampleStartTime, Date sampleEndTime,
                                         Double harmonicValue, Double medianValue, String unitsOfMeasurement) {
        super(id, name, sampleStartTime, sampleEndTime);
        this.harmonicValue = harmonicValue;
        this.medianValue = medianValue;
        this.unitsOfMeasurement = unitsOfMeasurement;
    }

    public Double getHarmonicValue() {
        return harmonicValue;
    }

    public void setHarmonicValue(Double harmonicValue) {
        this.harmonicValue = harmonicValue;
    }

    public Double getMedianValue() {
        return medianValue;
    }

    public void setMedianValue(Double medianValue) {
        this.medianValue = medianValue;
    }

    public String getUnitsOfMeasurement() {
        return unitsOfMeasurement;
    }

    public void setUnitsOfMeasurement(String unitsOfMeasurement) {
        this.unitsOfMeasurement = unitsOfMeasurement;
    }
}
