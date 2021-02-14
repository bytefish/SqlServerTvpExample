// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.sqlservertvpexample;

import de.bytefish.sqlservertvpexample.data.DataGenerator;
import de.bytefish.sqlservertvpexample.model.DeviceMeasurement;
import de.bytefish.sqlservertvpexample.processor.DeviceMeasurementBulkProcessor;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

public class Program {

	private static final String connectionUrl = "jdbc:sqlserver://localhost;instanceName=SQLEXPRESS;databaseName=SampleDatabase;user=philipp;password=test_pwd";

	public static void main(String[] args) {

		LocalDateTime startDate = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
		LocalDateTime endDate = LocalDateTime.of(2020, 12, 31, 0, 0, 0);

		// Create the Batch Processor:
		DeviceMeasurementBulkProcessor processor = new DeviceMeasurementBulkProcessor();

		// Generate a Stream of data using a fake device sending each 15s for a year. This should
		// generate something around 2 Million measurements:
		Stream<DeviceMeasurement> measurementStream = new DataGenerator(startDate, endDate, Duration.ofSeconds(15))
				.generate("device1", "parameter1", 10, 19);

		// Write Data in 80000 Value Batches:
		Disposable disposable = Observable
				.fromStream(measurementStream)
				.buffer(80_000)
				.forEach(values -> writeBatch(processor, values));

		// Being a good citizen by cleaning up RxJava stuff:
		disposable.dispose();
	}

	private static void writeBatch(DeviceMeasurementBulkProcessor processor, List<DeviceMeasurement> values) throws Exception {
		try {
			internalWriteBatch(processor, values);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void internalWriteBatch(DeviceMeasurementBulkProcessor processor, List<DeviceMeasurement> values) throws Exception {
		try (Connection connection = DriverManager.getConnection(connectionUrl)) {
			processor.saveAll(connection, values);
		}
	}

}