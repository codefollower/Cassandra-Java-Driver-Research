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

public class SizeTieredCompactionTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new SizeTieredCompactionTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        tableName = "SizeTieredCompactionTest";
        // create();
        insert();
        select();
    }

    void create() throws Exception {
        cql = "CREATE TABLE IF NOT EXISTS " + tableName + " (id int PRIMARY KEY, f1 int, f2 text)" + //
                "WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', " + //
                // 4个公共选项在AbstractCompactionStrategy中定义并由AbstractCompactionStrategy验证
                "'tombstone_threshold' : 0.2, 'tombstone_compaction_interval' : 86400, " + //
                "'unchecked_tombstone_compaction' : 'false', 'enabled' : 'true', " + //
                // 这两选项被忽略
                "'min_threshold' : 6, 'max_threshold' : 16, " + //
                // SizeTieredCompactionStrategy专属选项
                "'min_sstable_size' : 52428800, 'bucket_low' : 0.5, 'bucket_high' : 1.5, 'cold_reads_to_omit' : 0.05}";
        tryExecute();
    }

    void insert() throws Exception {
        int count = 7;
        for (int i = 0; i < count; i++) {
            cql = "INSERT INTO " + tableName + "(id, f1, f2) " + //
                    "VALUES (" + i + ", " + i + ", 'T" + i + "')";
            SimpleStatement stmt = newSimpleStatement(cql);
            stmt.setConsistencyLevel(ConsistencyLevel.TWO);
            stmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
            stmt.setConsistencyLevel(ConsistencyLevel.ONE);
            execute(stmt);
        }
    }

    void select() {
        cql = "select * from " + tableName + " where id in (1,2,3)";
        ResultSet rs = session.execute(cql);
        // rs.all();
        for (Row row : rs)
            System.out.println(row);
    }
}
