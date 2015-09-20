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

public class JsonTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new JsonTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        // create();
        // insert();

        select();
    }

    void create() throws Exception {

        cql = "CREATE FUNCTION IF NOT EXISTS f_sin ( input double ) CALLED ON NULL INPUT RETURNS double "
                + "LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'";
        execute();

        execute("CREATE TYPE IF NOT EXISTS fullname (firstname text, lastname text)");
        execute("CREATE TYPE IF NOT EXISTS fullname2 (f2 frozen<fullname>, lastname2 text)");
        execute("CREATE TYPE IF NOT EXISTS fullname3 (f3 frozen<fullname2>, lastname3 text)");
        execute("DROP TABLE IF EXISTS users");
        execute("CREATE TABLE IF NOT EXISTS users " + //
                "(id int, f1 int, age int, f3 int, f4 int, f2 double, first_name text,last_name text," + //
                "f_set set<text>, f_list list<text>, f_map map<text, text>, " + //
                "name frozen <fullname>, " + //
                // "f_static int static, " + //
                "f_tuple frozen<tuple<int, text, float>>, " + //
                "f_f3 frozen<fullname3>, " + //
                // "PRIMARY KEY ((id,f1),age, f3, f4))");
                // "PRIMARY KEY (id,f1))");
                "PRIMARY KEY ((id,f1),f2,f3))");
    }

    void insert() throws Exception {
        cql = "insert into users json '{\"id\":1, \"f1\":2, \"f2\":3, \"f3\":3," + //
                "\"f_f3\": {" + //
                "\"f3\": {" + //
                "\"f2\": {" + //
                "\"firstname\":\"ab\"," + //
                "\"lastname\":\"ab\"" + //
                " }," + //
                "\"lastname2\":\"ab\"" + //
                " }," + //
                "\"lastname3\":\"ab\"" + //
                " }" + //
                "}'";
        execute();
        // int count = 10;
        // for (int i = 1; i < count; i++) {
        // cql = "insert into users(id, f1, f2, f3, name) values(" + i + ", 1,2,3,('abc','def'))";
        // execute();
        // }
    }

    void select() throws Exception {
        cql = "select id, name.firstname as fn, ttl(name), writetime(name),f_sin(f2) from users";
        printResultSet();
        cql = "select id, sum(f3) from users";
        printResultSet();

        cql = "select json * from users where id=1 and f1=2 and f_f3.f3.f2.firstname='ab'";

        cql = "select f_f3.f3.lastname2, f_f3.f3.f2.firstname from users where id=1 and f1=2";

        // cql = "select json id, name as n from users where id=1 and f1=1 limit 3";
        printResultSet();
    }
}
