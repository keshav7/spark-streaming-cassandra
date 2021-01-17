package org.example.strreaming;


import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.example.strreaming.model.OrderEventAggregate;

public class CassandraDBWriterManager extends ForeachWriter<Row> {

    private static CassandraDBManager cassandraDBManager = null;

    @Override
    public boolean open(long partitionId, long epochId) {

        if(cassandraDBManager == null) {
            cassandraDBManager = new CassandraDBManager();
            cassandraDBManager.setUp();
        }

        return true;
    }

    @Override
    public void process(Row value) {

        cassandraDBManager.insertOrderEventAggregate(OrderEventAggregate.builder()
                .eventName(value.getString(1))
                .timestamp(((GenericRowWithSchema)value.get(0)).get(0).toString())
                .totalCount((int) value.getLong(2))
                .build());

    }

    @Override
    public void close(Throwable errorOrNull) {

    }
}
