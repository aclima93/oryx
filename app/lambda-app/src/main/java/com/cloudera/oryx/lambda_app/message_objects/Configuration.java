package com.cloudera.oryx.lambda_app.message_objects;

import java.util.Date;

/**
 * Created by aclima on 13/12/2016.
 */

public class Configuration {

    private String name;
    private String value;
    private Date timestamp;

    public Configuration(String name, String value, Date timestamp) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}