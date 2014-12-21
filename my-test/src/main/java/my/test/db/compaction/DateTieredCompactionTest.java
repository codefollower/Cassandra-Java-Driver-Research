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
package my.test.db.compaction;

import my.test.TestBase;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

public class DateTieredCompactionTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new DateTieredCompactionTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        tableName = "DateTieredCompactionTest";
        //create();
        insert();
        select();
    }

    void create() throws Exception {
        cql = "CREATE TABLE IF NOT EXISTS " + tableName + " (id int PRIMARY KEY, f1 int, f2 text)" + //
                "WITH compaction = { 'class' : 'DateTieredCompactionStrategy', " + //
                //4个公共选项在AbstractCompactionStrategy中定义并由AbstractCompactionStrategy验证
                "'tombstone_threshold' : 0.2, 'tombstone_compaction_interval' : 86400, " + //
                "'unchecked_tombstone_compaction' : 'false', 'enabled' : 'true', " + //
                //这两选项被忽略
                "'min_threshold' : 6, 'max_threshold' : 16, " + //
                //DateTieredCompactionStrategy专属选项
                "'timestamp_resolution' : 'MICROSECONDS', 'max_sstable_age_days' : 365, 'base_time_seconds' : 3600}";
        tryExecute();
    }

    void insert() throws Exception {
        int count = 20;
        for (int i = 0; i < count; i++) {
            cql = "INSERT INTO " + tableName + "(id, f1, f2) " + //
                    "VALUES (" + i + ", " + i + ", 'T" + i + "')";
            SimpleStatement stmt = new SimpleStatement(cql);
            stmt.setConsistencyLevel(ConsistencyLevel.TWO);
            stmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
            stmt.setConsistencyLevel(ConsistencyLevel.ONE);
            execute(stmt);
        }
    }

    void select() {
        cql = "select * from " + tableName + " where id in (1,2,3)";
        ResultSet rs = session.execute(cql);
        //rs.all();
        for (Row row : rs)
            System.out.println(row);
    }
}
