package de.bytefish.sqlservertvpexample.processor;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import de.bytefish.sqlservertvpexample.model.DeviceMeasurement;
import de.bytefish.sqlservertvpexample.utils.Tuple3;

import java.sql.Connection;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class DeviceMeasurementBulkProcessor {

    private static final String SQL_COMMAND = "{call [sample].[InsertOrUpdateDeviceMeasurements] (?)}";

    public void saveAll(Connection connection, Collection<DeviceMeasurement> source) throws Exception {

        // In a Batch the Values have to be unique to satisfy the Unique Constraint in the Database,
        // so we group them by multiple keys and then take the first value from each of the batches:
        List<DeviceMeasurement> distinctDeviceMeasurements = source.stream()
                .collect(groupingBy(x -> new Tuple3<>(x.getDeviceId(), x.getParameterId(), x.getTimestamp())))
                .values().stream()
                .map(x -> x.get(0))
                .collect(Collectors.toList());

        // Build the SQLServerDataTable:
        SQLServerDataTable sqlServerDataTable = buildSqlServerDataTable(distinctDeviceMeasurements);

        // And insert it:
        try (SQLServerCallableStatement  callableStmt  = (SQLServerCallableStatement) connection.prepareCall(SQL_COMMAND)) {
            callableStmt.setStructured(1, "[sample].[DeviceMeasurementType]", sqlServerDataTable);
            callableStmt.execute();
        }
    }

    private SQLServerDataTable buildSqlServerDataTable(Collection<DeviceMeasurement> deviceMeasurements) throws SQLServerException {
        SQLServerDataTable tvp = new SQLServerDataTable();

        tvp.addColumnMetadata("DeviceID", Types.NVARCHAR);
        tvp.addColumnMetadata("ParameterID", Types.NVARCHAR);
        tvp.addColumnMetadata("Timestamp", Types.TIMESTAMP);
        tvp.addColumnMetadata("Value", Types.DECIMAL);

        for (DeviceMeasurement deviceMeasurement : deviceMeasurements) {
            tvp.addRow(
                    deviceMeasurement.getDeviceId(),
                    deviceMeasurement.getParameterId(),
                    deviceMeasurement.getTimestamp(),
                    deviceMeasurement.getValue()
            );
        }

        return tvp;
    }
}
