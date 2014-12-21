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
package my.test.cql3;

import my.test.TestBase;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

public class BatchStatementTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new BatchStatementTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        tableName = "BatchStatementTest";
        createTable();

        insert();
        select();
    }

    void createTable() throws Exception {
        cql = "CREATE TABLE IF NOT EXISTS " + tableName //
                + " ( block_id int, short_hair boolean, f1 text, " //
                + "PRIMARY KEY (block_id, short_hair)) WITH compaction = { 'class' : 'LeveledCompactionStrategy'}";

        execute(cql);
    }

    void select() {
        cql = "select * from " + tableName + " where block_id in (1,2,3)";
        ResultSet rs = session.execute(cql);
        for (Row row : rs)
            System.out.println(row);
    }

    void insert() throws Exception {
        cql = " INSERT INTO " + tableName + "(block_id, short_hair, f1) VALUES (1, true, 'ab')";
        BatchStatement stmt = new BatchStatement(BatchStatement.Type.LOGGED);
        stmt.add(new SimpleStatement(cql));
        cql = " INSERT INTO " + tableName + "(block_id, short_hair, f1) VALUES (2, true, 'ab')";
        stmt.add(new SimpleStatement(cql));

        session.execute(stmt);

        execute(" BEGIN BATCH " + //
                " INSERT INTO " + tableName + "(block_id, short_hair, f1) VALUES (3, true, 'ab')" + //
                " INSERT INTO " + tableName + "(block_id, short_hair, f1) VALUES (4, true, 'cd')" + //
                " APPLY BATCH");
    }
}
