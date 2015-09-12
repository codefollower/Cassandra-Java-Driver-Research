/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package my.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;

public abstract class TestBase {
    protected static final String KEYSPACE_NAME = "mytest";
    protected String tableName;
    protected String cql;
    protected Cluster cluster;
    protected Session session;
    protected String address = "127.0.0.1";

    private void initDefaults() throws Exception {
        Cluster.Builder builder = Cluster.builder().addContactPoint(address);
        // builder.addContactPoint("127.0.0.1");
        // builder.withClusterName("My Cassandra Cluster");
        SocketOptions so = new SocketOptions();
        // 设置一下这两个参数，避免在用eclipse进行debug代码时出现超时
        so.setReadTimeoutMillis(60 * 60 * 1000);
        so.setConnectTimeoutMillis(60 * 60 * 1000);
        builder.withSocketOptions(so);

        builder.withCredentials("cassandra", "cassandra");

        PoolingOptions po = new PoolingOptions();
        po.setCoreConnectionsPerHost(HostDistance.LOCAL, 1);
        po.setCoreConnectionsPerHost(HostDistance.REMOTE, 1);
        builder.withPoolingOptions(po);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.ONE);
        // queryOptions.setConsistencyLevel(ConsistencyLevel.EACH_QUORUM);

        builder.withQueryOptions(queryOptions);

        builder.withCompression(Compression.SNAPPY);
        cluster = builder.build();
        cluster.init();

        session = cluster.connect();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':3};");

        // session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME + " WITH replication "
        // + "= {'class':'NetworkTopologyStrategy', 'DC1':2, 'DC2':1};");
        session.execute("USE " + KEYSPACE_NAME);
    }

    public void init() throws Exception {
    }

    public void start() throws Exception {
        try {
            initDefaults();
            init();

            startInternal();

        } finally {
            stop();
        }

    }

    public void stop() throws Exception {
        if (session != null)
            session.close();
        if (cluster != null)
            cluster.close();
    }

    public abstract void startInternal() throws Exception;

    public void execute() {
        execute(cql);
    }

    public PreparedStatement prepare(String cql) {
        return session.prepare(cql);
    }

    public ResultSet execute(String cql) {
        return session.execute(cql);
    }

    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    public void tryExecute() {
        tryExecute(cql);
    }

    public void tryExecute(String cql) {
        try {
            session.execute(cql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void tryPrintResultSet() {
        try {
            printResultSet();
        } catch (Exception e) {
            System.out.println("***Exception***: " + e.getMessage());
        }
    }

    public void printResultSet() {
        ResultSet results = session.execute(cql);
        printResultSet(results);
    }

    public void printResultSet(ResultSet rs) {
        for (Row row : rs)
            System.out.println(row);
    }

    public void tryPrintResultSet(ResultSet rs) {
        try {
            printResultSet(rs);
        } catch (Exception e) {
            System.out.println("***Exception***: " + e.getMessage());
        }
    }

    public void printResultSet2(ResultSet results) {
        ColumnDefinitions cd = results.getColumnDefinitions();
        int size = cd.size();

        for (Row row : results) {
            for (int i = 0; i < size; i++) {
                System.out.print(" ");
                DataType dt = cd.getType(i);
                switch (dt.getName()) {
                case TEXT:
                    System.out.print(row.getString(i));
                    break;
                case INT:
                    System.out.print(row.getInt(i));
                    break;
                case BIGINT:
                    System.out.print(row.getLong(i));
                    break;
                case UUID:
                    System.out.print(row.getUUID(i));
                    break;
                case SET:
                    System.out.print(row.getSet(i, String.class));
                    break;
                case LIST:
                    System.out.print(row.getList(i, String.class));
                    break;
                case MAP:
                    System.out.print(row.getMap(i, java.util.Date.class, String.class));
                    break;
                case COUNTER:
                    System.out.print(row.getLong(i));
                    break;
                case BOOLEAN:
                    System.out.print(row.getBool(i));
                    break;
                case DOUBLE:
                    System.out.print(row.getDouble(i));
                    break;
                default:
                    System.out.print(row.getString(i));
                    break;
                }
            }
            System.out.println();
        }
        System.out.println();
    }

    public SimpleStatement newSimpleStatement(String cql) {
        return session.newSimpleStatement(cql);
    }
}
