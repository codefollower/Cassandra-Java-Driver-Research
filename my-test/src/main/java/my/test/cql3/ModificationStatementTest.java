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

public class ModificationStatementTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new ModificationStatementTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        // create();
        testParsedInsert();
    }

    void create() throws Exception {
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
    }

    void testParsedInsert() throws Exception {
        // 使用if not exists时不能用timestamp
        cql = "insert into users(id,f1,f2,f3) values(1,2,3,4) if not exists USING timestamp 10 and ttl 20";
        cql = "insert into users(id,f1,f2,f3) values(1,2,3,4) if not exists USING ttl 20";

        cql = "insert into users(id,f1,f2,f3) values(1,2,3,4) USING timestamp 10 and ttl 20";
        execute();
    }
}
