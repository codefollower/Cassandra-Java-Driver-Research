/*
 * Copyright 2011 The Apache Software Foundation
 *
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

public class TypeTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new TypeTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        //dropTest();
        //createTest();
        alterTest();
    }

    void createTest() throws Exception {
        //        execute("CREATE TYPE IF NOT EXISTS TypeTest(myint int, mytext text)");
        //
        //        execute("CREATE TYPE IF NOT EXISTS fullname (firstname text, lastname text)");
        //
        //        tryExecute("CREATE TABLE IF NOT EXISTS TypeTest_Table ( id int primary key, name fullname)");
        //
        //        //tryExecute("INSERT INTO TypeTest_Table(id, name) VALUES (1, {'a','b'})");
        //
        //        tryExecute("INSERT INTO TypeTest_Table(id, name.firstname, name.lastname) VALUES (1, 'a','b')");
        //

        //与set和map都用相同的大号括语法
        //但是因为UserType的语法是usertype_literal以cident(标识符)开头，只要预读一些字符，就能区分set和map，set和map中的值都是常量
        //见src/java/org/apache/cassandra/cql3/Cql.g文件
        execute("INSERT INTO TypeTest_Table(id, name) VALUES (1, { firstname: 'a', lastname: 'b'})");

        cql = "SELECT id, name.firstname FROM TypeTest_Table where id=1";
        printResultSet();

        //错误
        cql = "SELECT id, name FROM TypeTest_Table where id=1";
        //printResultSet();
    }

    void alterTest() throws Exception {
        //tryExecute("ALTER TYPE TypeTest ALTER myint TYPE boolean");
        //execute("ALTER TYPE TypeTest ADD myfloat float");

        execute("DROP TYPE IF EXISTS type1");
        execute("CREATE TYPE IF NOT EXISTS type1(f1 int, f2 text, f3 text)");

        //execute("ALTER TYPE type1 RENAME TO type2");

        execute("ALTER TYPE type1 RENAME f1 TO f11");
        execute("ALTER TYPE type1 RENAME f2 TO f22 AND f3 TO f33");

        //execute("ALTER TYPE TypeTest RENAME myint TO mybigint AND myfloat TO myf");
    }

    void dropTest() throws Exception {
        execute("DROP TYPE IF EXISTS TypeTest");
    }

}
