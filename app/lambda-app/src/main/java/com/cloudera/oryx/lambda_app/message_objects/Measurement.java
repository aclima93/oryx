package com.cloudera.oryx.lambda_app.message_objects;

/**
 * Created by aclima on 13/12/2016.
 */

public class Measurement {

    public static final String DELIMITER = " - ";

    private Integer id;
    private String name;

    public Measurement(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

}
