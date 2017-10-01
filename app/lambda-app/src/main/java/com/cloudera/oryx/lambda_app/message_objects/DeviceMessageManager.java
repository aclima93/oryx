package com.cloudera.oryx.lambda_app.message_objects;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

public class DeviceMessageManager {

    public HashMap<Measurement, Integer> getDistinctMeasurementsFromDeviceMessageJSON(String deviceMessageJSON){

        // get the message object
        DeviceMessage deviceMessage;
        deviceMessage = getDeviceMessageFromDeviceMessageJson(deviceMessageJSON);

        // return the distinct measurements Set
        return getDistinctMeasurementsFromDeviceMessage(deviceMessage);
    }

    public ArrayList<Measurement> getMeasurementsFromDeviceMessageJSON(String deviceMessageJSON){

        // get the message object
        DeviceMessage deviceMessage;
        deviceMessage = getDeviceMessageFromDeviceMessageJson(deviceMessageJSON);

        // return the measurements ArrayList
        return getMeasurementsFromDeviceMessage(deviceMessage);
    }

    public DeviceMessage getDeviceMessageFromDeviceMessageJson(String deviceMessageJSON){

        // get the message object from the JSON string
        final Gson gson = new Gson();
        return gson.fromJson(deviceMessageJSON, DeviceMessage.class);
    }

    private HashMap<Measurement,Integer> measurementArrayListToOccurrenceHashMap(ArrayList<Measurement> measurements){
        HashMap<Measurement,Integer> occurrenceHashMap = new HashMap<>();

        for(Measurement measurement : measurements){

            if( !occurrenceHashMap.containsKey(measurement) ){
                // object not yet in hashmap
                occurrenceHashMap.put(measurement, 1);
            }
            else{
                // increment occurrence counter
                occurrenceHashMap.put(measurement, occurrenceHashMap.get(measurement) + 1 );
            }
        }
        return occurrenceHashMap;
    }

    private HashMap<Measurement, Integer> getDistinctMeasurementsFromDeviceMessage(DeviceMessage deviceMessage){

        // get the measurements ArrayList
        ArrayList<Measurement> measurements = getMeasurementsFromDeviceMessage(deviceMessage);

        // return the distinct measurements Set
        return measurementArrayListToOccurrenceHashMap(measurements);
    }

    private ArrayList<Measurement> getMeasurementsFromDeviceMessage(DeviceMessage deviceMessage){

        final Gson gson = new Gson();
        Type listType;
        ArrayList<Measurement> measurements;

        // the payload itself is also a JSON string representing a list of measurements for a particular class/topic
        switch (deviceMessage.getSubtopic()) {

            case "PeriodicMeasurement":
                listType = new TypeToken<ArrayList<PeriodicMeasurement>>() {
                }.getType();
                measurements = gson.fromJson(deviceMessage.getPayload(), listType);
                break;

            case "EventfulMeasurement":
                listType = new TypeToken<ArrayList<EventfulMeasurement>>() {
                }.getType();
                measurements = gson.fromJson(deviceMessage.getPayload(), listType);
                break;

            case "OneTimeMeasurement":
                listType = new TypeToken<ArrayList<OneTimeMeasurement>>() {
                }.getType();
                measurements = gson.fromJson(deviceMessage.getPayload(), listType);
                break;

            case "PeriodicAggregatedMeasurement":
                listType = new TypeToken<ArrayList<PeriodicAggregatedMeasurement>>() {
                }.getType();
                measurements = gson.fromJson(deviceMessage.getPayload(), listType);
                break;

            case "EventfulAggregatedMeasurement":
                listType = new TypeToken<ArrayList<EventfulAggregatedMeasurement>>() {
                }.getType();
                measurements = gson.fromJson(deviceMessage.getPayload(), listType);
                break;

            default:
                measurements = new ArrayList<>();
                break;
        }

        return measurements;
    }
}
