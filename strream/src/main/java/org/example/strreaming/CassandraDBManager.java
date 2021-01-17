package org.example.strreaming;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.example.strreaming.model.OrderEventAggregate;

import java.io.Serializable;

public class CassandraDBManager implements Serializable {

    private Cluster cluster;
    private Session session;

    private static int counter = 1;

    public void setUp() {

        this.cluster = Cluster.builder().withoutJMXReporting().addContactPoint("127.0.0.1").withPort(9042).build();
        this.session = cluster.connect();

    }

    public void insertOrderEventAggregate(OrderEventAggregate orderEventAggregate) {

        session.execute("insert into sparkk.order_event_aggreg (event_name, timestamp, total_count) values (" +
                "'" + orderEventAggregate.getEventName() + "'" + "," +
                "'" + orderEventAggregate.getTimestamp() + "'" + "," +
                orderEventAggregate.getTotalCount() + ");");

    }



}
