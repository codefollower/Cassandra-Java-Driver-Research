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

public class ConstantsTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new ConstantsTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        test_Adder_Substracter();
        dropTest();
    }

    void test_Adder_Substracter() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS test_Adder (counter_value counter," + //
                "url_name varchar,," + //
                "page_name varchar,," + //
                "PRIMARY KEY (url_name, page_name))");
        execute("UPDATE test_Adder SET counter_value = counter_value + 1 WHERE url_name='datastax.com' AND page_name='home'");

        execute("UPDATE test_Adder SET counter_value = counter_value - 2 WHERE url_name='datastax.com' AND page_name='home'");

        //出错:The negation of -9223372036854775808 overflows supported counter precision (signed 8 bytes integer)
        //不允许是Long.MIN_VALUE
        tryExecute("UPDATE test_Adder SET counter_value = counter_value - -9223372036854775808 "
                + "WHERE url_name='datastax.com' AND page_name='home'");
    }

    void dropTest() throws Exception {
    }

}
