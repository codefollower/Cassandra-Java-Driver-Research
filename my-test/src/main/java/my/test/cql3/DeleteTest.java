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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

public class DeleteTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new DeleteTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        tableName = "DeleteTest";
        //create();
        //insert();
        delete();
        //select();

        //testDense();
    }

    void testDense() throws Exception {
        tableName = "DeleteTest2";

        cql = "CREATE TABLE IF NOT EXISTS " + tableName //
                + " ( id int, f2 int, f3 int, f_set set<int>, f_list list<int>, f_map map<int, int>, " //
                + "PRIMARY KEY (id, f2, f3)) WITH COMPACT STORAGE";
        //execute(cql);

        cql = "INSERT INTO " + tableName + "(id, f2, f3) VALUES (1, 2, 3)";
        execute();
    }

    void select() {
        cql = "select * from " + tableName + " where id in (1,2,3)";
        ResultSet rs = session.execute(cql);
        //rs.all();
        for (Row row : rs)
            System.out.println(row);
    }

    void create() throws Exception {
        //execute("DROP TABLE IF EXISTS " + tableName);

        cql = "CREATE TABLE IF NOT EXISTS " + tableName //
                + " ( id int, short_hair boolean, f1 text, f2 int, f3 int, " //
                + "   f_set set<int>, f_list list<int>, f_map map<int, int>, " //
                + "PRIMARY KEY (id, f2, f3))";
        execute(cql);
    }

    void insert() throws Exception {
        int count = 6;
        for (int i = 0; i < count; i++) {
            cql = "INSERT INTO " + tableName + "(id, short_hair, f1, f2, f3, f_set, f_list, f_map) " + //
                    "VALUES (" + i + ", true, 'abbc', 1, 2, {1, 2, 3}, [4,5,6], {1:10, 2:20, 3:30})";
            SimpleStatement stmt = new SimpleStatement(cql);
            stmt.setConsistencyLevel(ConsistencyLevel.ONE);
            //stmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
            execute(stmt);
        }
        //        cql = "INSERT INTO " + tableName + "(id, short_hair, f1, f2, f3) VALUES (1, true, 'abbc', 1, 2)";
        //        SimpleStatement stmt = new SimpleStatement(cql);
        //        stmt.setConsistencyLevel(ConsistencyLevel.ONE);
        //        //stmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
        //        execute(stmt);
        //
        //        cql = "INSERT INTO " + tableName
        //                + "(id, short_hair, f1, f2, f3) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ? AND TTL ?";
        //        PreparedStatement statement = session.prepare(cql);
        //        BoundStatement boundStatement = new BoundStatement(statement);
        //        session.execute(boundStatement.bind(1, true, "ab", 3, 4, 10000L, 100));
    }

    void delete() throws Exception {
        //必需指定所有的partition_key和clustering_column
        cql = "DELETE f_set[1] FROM " + tableName + " WHERE id=1";
        tryExecute();
        cql = "DELETE f_set[1] FROM " + tableName + " WHERE id=1 and f2=1";
        tryExecute();
        cql = "DELETE f_set[1] FROM " + tableName + " WHERE f2=1 and f3=2";
        tryExecute();
        cql = "DELETE f_set[1] FROM " + tableName + " WHERE id=1 and f2=1 and f3=2";
        execute();

        //不允许删除多个元素
        cql = "DELETE f_set[1,2] FROM " + tableName + " WHERE id=1 and f2=1 and f3=2";
        tryExecute();

        //可以用这种方式删除多个元素
        cql = "UPDATE " + tableName + " SET f_set = f_set - {1, 2} WHERE id=1 and f2=1 and f3=2";
        tryExecute();

        //execute("DELETE f1, f_list[0] FROM " + tableName + " WHERE id=2 AND f2=1 AND f3=2");

        //execute("DELETE FROM " + tableName + " WHERE id=2");
        //execute("DELETE FROM " + tableName + " WHERE id=1 AND f2=1 AND f3=2 IF EXISTS");
    }

    void delete2() throws Exception {
        //还不支持在where中使用or
        cql = "DELETE f1 FROM " + tableName + " WHERE id=1 or id=2";
        //where中只允许出现primary key
        //否则出错:Non PRIMARY KEY f1 found in where clause
        cql = "DELETE f1 FROM " + tableName + " WHERE f1='abbc'";
        cql = "DELETE f1 FROM " + tableName + " WHERE id=1";

        cql = "DELETE f1 FROM " + tableName + " WHERE id=1 and f2=3 and f3=4";
        cql = "DELETE FROM " + tableName + " WHERE id=1 and f2=3 and f3=4";
        //where中必须指定PARTITION_KEY
        //cql = "DELETE FROM " + tableName + " WHERE f2=3 and f3=4";

        //PARTITION_KEY和CLUSTERING_COLUMN不能出现在if子句中
        cql = "DELETE FROM " + tableName + " WHERE id=1 and f2=3 and f3=4 if f2=3 and f3=4";
        cql = "DELETE FROM " + tableName + " WHERE id=1 and f2=3 and f3=4 if short_hair=true and f1='abc'";
        execute();

        cql = "DELETE FROM " + tableName + " WHERE id=1";
        //cql = "DELETE FROM " + tableName; //必须指定where
        execute();

        //错误:IN on the partition key is not supported with conditional updates
        tryExecute("DELETE FROM " + tableName + " WHERE id in(1,2,3) IF f1='ab' AND short_hair=true");

        execute("DELETE FROM " + tableName + " WHERE id=1 IF f1='ab' AND short_hair=true");

        tableName = "DeleteTest2";
        cql = "CREATE TABLE IF NOT EXISTS " + tableName //
                + " ( id int, short_hair boolean, f1 text, f2 text, f3 text, " //
                + "PRIMARY KEY (id, f1, f2))";
        execute(cql);
        cql = "INSERT INTO " + tableName + "(id, short_hair, f1, f2, f3) VALUES (1, true, 'abbc', 'abbc', 'abbc')";
        execute(cql);

        //对应一个RangeTombstone
        execute("DELETE FROM " + tableName + " WHERE id=1 AND f1='abbc'");

        //错误:Missing mandatory PRIMARY KEY part f2 since f3 specified
        tryExecute("DELETE f3 FROM " + tableName + " WHERE id=1 AND f1='abbc'");

        tableName = "DeleteTest3";
        cql = "CREATE TABLE IF NOT EXISTS " + tableName //
                + " ( id int, short_hair boolean, f1 text, f2 text, f3 text, " //
                + "PRIMARY KEY (id, f1, f2, short_hair)) WITH COMPACT STORAGE";
        execute(cql);
        cql = "INSERT INTO " + tableName + "(id, short_hair, f1, f2, f3) VALUES (1, true, 'abbc', 'abbc', 'abbc')";
        execute(cql);

        //对应org.apache.cassandra.cql3.statements.DeleteStatement.updateForKey(ByteBuffer, ColumnNameBuilder, UpdateParameters)
        //的if (isRange)
        execute("DELETE FROM " + tableName + " WHERE id=1 AND f1='abbc'");

        //对应org.apache.cassandra.cql3.statements.DeleteStatement.updateForKey(ByteBuffer, ColumnNameBuilder, UpdateParameters)
        //的if (cfDef.isCompact)
        execute("DELETE FROM " + tableName + " WHERE id=1 AND f1='abbc' AND f2='abbc' AND short_hair=true");
    }
}
