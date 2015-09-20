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
package my.test.cql3.ddl;

import my.test.TestBase;

public class FunctionTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new FunctionTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        // 不能在system中建函数
        // execute("use system");
        // 也不能像这样system.f_sin
        cql = "CREATE OR REPLACE FUNCTION f_sin ( input frozen<set<double>> ) CALLED ON NULL INPUT RETURNS double "
                + "LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'";
        execute();

        cql = "DROP FUNCTION IF EXISTS f_sin";
        execute();
    }

}
