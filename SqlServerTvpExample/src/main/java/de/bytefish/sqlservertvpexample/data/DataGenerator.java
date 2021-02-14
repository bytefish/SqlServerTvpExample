package de.bytefish.sqlservertvpexample.data;


import de.bytefish.sqlservertvpexample.model.DeviceMeasurement;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DataGenerator {

    private static Random random = new Random();

    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private Duration interval;

    public DataGenerator(LocalDateTime startDate, LocalDateTime endDate, Duration interval) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.interval = interval;
    }

    public Stream<DeviceMeasurement> generate(final String deviceId, final String parameterId, final double low, final double high) {

        // For Creating the Measurement TimeSteps:
        final DateTimeIterator iterator = new DateTimeIterator(startDate, endDate, interval);

        // Create the Stream:
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(measurementTimeStamp -> createDeviceMeasurement(deviceId, parameterId, measurementTimeStamp, low, high));
    }

    private DeviceMeasurement createDeviceMeasurement(final String deviceId, final String parameterId, final LocalDateTime timestamp, final double low, final double high) {

        // Generate a Random Value for the Sensor:
        final double randomValue = low + (high - low) * random.nextDouble();

        // Create the Measurement:
        final DeviceMeasurement data = new DeviceMeasurement();

        data.setDeviceId(deviceId);
        data.setParameterId(parameterId);
        data.setTimestamp(Timestamp.valueOf(timestamp));
        data.setValue(randomValue);

        return data;
    }
}