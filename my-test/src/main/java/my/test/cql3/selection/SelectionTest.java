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
package my.test.cql3.selection;

import my.test.TestBase;

public class SelectionTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new SelectionTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        // cql = "DROP INDEX  IF EXISTS users_id ";
        // execute();

        // create();
        // insert();

        // select();
        // test_makeClusteringIndexFilter();
        // test_getRowFilter();

        // test_SinglePartitionNamesCommand_queryMemtableAndDiskInternal();
        // test_SinglePartitionSliceCommand_queryMemtableAndDiskInternal();

        // test_QueryPager();

        // test_StorageProxy_fetchRows();
        test_StorageProxy_getRangeSlice();
    }

    void create() throws Exception {

        cql = "CREATE FUNCTION IF NOT EXISTS f_sin ( input double ) CALLED ON NULL INPUT RETURNS double "
                + "LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'";
        execute();

        execute("CREATE TYPE IF NOT EXISTS fullname (firstname text, lastname text)");
        execute("DROP TABLE IF EXISTS users");
        execute("CREATE TABLE IF NOT EXISTS users " + //
                "(id int, f1 int, age int, f3 int, f4 int, f2 double, first_name text,last_name text," + //
                "f_set set<text>, f_list list<text>, f_map map<text, text>, " + //
                "name frozen <fullname>, " + //
                // "f_static int static, " + //
                "f_tuple frozen<tuple<int, text, float>>, " + //
                // "PRIMARY KEY ((id,f1),age, f3, f4))");
                // "PRIMARY KEY (id,f1))");
                "PRIMARY KEY ((id,f1),f2,f3))");

        cql = "CREATE INDEX IF NOT EXISTS users_id ON users (id)"; // 有索引后，就可以在where中只出现id了，否则需要id、f1同时出现
        execute();
    }

    void insert() throws Exception {
        int count = 10;
        // for (int i = 1; i < count; i++) {
        // cql = "insert into users(id, f1, f2, f3, name) values(" + i + ", 1,2,3,('abc','def'))";
        // execute();
        // }

        count = 20;
        // for (int i = 10; i < count; i++) {
        // cql = "insert into users(id, f1, f2, f3, name) values(" + i + "," + i + "," + i + "," + i
        // + ",('abc','def'))";
        // execute();
        // }

        for (int i = 10; i < count; i++) {
            cql = "insert into users(id, f1, f2, f3, name) values(10,10," + i + "," + i + ",('abc','def'))";
            execute();
        }
    }

    void select() throws Exception {
        cql = "select id, name.firstname as fn, ttl(name), writetime(name),f_sin(f2) from users";
        printResultSet();
        cql = "select id, sum(f3) from users";
        printResultSet();

        cql = "select json * from users";
        cql = "select json id, name as n from users where id=1 and f1=1 limit 3";
        printResultSet();
    }

    void test_makeClusteringIndexFilter() {
        cql = "select distinct id, f1 from users";
        printResultSet();

        cql = "select id, f1, name from users where id=1 and f1=1 and f2=2";
        printResultSet();

        cql = "select id, f1, name from users where id=1 and f1=1 and f2=2 and f3=3";
        printResultSet();

        cql = "select id, f1, name from users where id=1 and f1=1 and (f2,f3) in ((2,3),(4,5))";
        printResultSet();
    }

    void test_getRowFilter() {
        // cql = "CREATE INDEX IF NOT EXISTS users_id ON users (id)";
        // execute();
        // cql = "CREATE INDEX IF NOT EXISTS users_f2 ON users (f2)";
        // execute();
        cql = "select id, f1, name from users where id=1 and f1=1 and f2=2 and f3=3 and f4=5";
        // printResultSet();

        // execute("CREATE TABLE IF NOT EXISTS users2 (id int primary key, f1 int)");
        cql = "insert into users2(id, f1) values(1,2)";
        // execute();

        // int count = 1000;
        // for (int i = 1; i < count; i++) {
        // cql = "insert into users2(id, f1) values(" + i + ", " + i + ")";
        // execute();
        // }

        cql = "select id, f1 from users2 where id=5";
        printResultSet();
    }

    void test_SinglePartitionNamesCommand_queryMemtableAndDiskInternal() {
        cql = "select id, f1, name from users where id=10 and f1=10 and (f2,f3) in ((10,10),(11,11))";
        printResultSet();
    }

    void test_SinglePartitionSliceCommand_queryMemtableAndDiskInternal() {
        cql = "select id, f1, name from users where id=10 and f1=10 and (f2,f3) >= (10,10) and (f2,f3) <= (11,11)";
        printResultSet();
    }

    void test_QueryPager() {
        // 测试SinglePartitionPager
        cql = "select id, f1, f2, f3, name from users where id=10 and f1=10";
        printResultSet(5); // fetchSize<=0时,默认变成5000，见com.datastax.driver.core.QueryOptions.DEFAULT_FETCH_SIZE

        // limit<=fetchSize就不分页了
        cql = "select id, f1, f2, f3, name from users where id=10 and f1=10 limit 2";
        printResultSet(5);

        // 测试MultiPartitionPager
        cql = "select id, f1, name from users where id=10 and f1 in(10, 11)";
        printResultSet(5);

        // 测试RangeNamesQueryPager
        cql = "select id, f1, name from users where id=10 and (f2,f3) in ((10,10),(11,11)) ALLOW FILTERING";
        printResultSet(5);

        // 测试RangeSliceQueryPager
        cql = "select id, f1, name from users where id=10 and (f2,f3) >= (10,10) and (f2,f3) <= (11,11) ALLOW FILTERING";
        printResultSet(5);

        cql = "select id, f1, name from users where id=10 and (f2,f3) >= (10,10) ALLOW FILTERING";
        // printResultSet(5);

    }

    void test_StorageProxy_fetchRows() {
        cql = "select id, f1, f2, f3, name from users where id=10 and f1=10 limit 2";
        printResultSet();
    }

    void test_StorageProxy_getRangeSlice() {
        cql = "select id, f1, name from users where id=10 and (f2,f3) in ((10,10),(11,11)) ALLOW FILTERING";

        cql = "select id, f1, name from users";
        printResultSet();
    }
}
