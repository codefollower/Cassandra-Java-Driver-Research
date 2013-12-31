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

public class PermissionTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new PermissionTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS PermissionTest ( f1 int primary key, f2 int)");
        tryExecute("CREATE USER User_PermissionTest WITH PASSWORD 'mypassword' SUPERUSER");
        execute("GRANT SELECT PERMISSION ON TABLE " + KEYSPACE_NAME + ".PermissionTest TO User_PermissionTest");
        execute("LIST SELECT PERMISSION  ON TABLE " + KEYSPACE_NAME + ".PermissionTest OF User_PermissionTest");
        execute("REVOKE SELECT PERMISSION ON TABLE " + KEYSPACE_NAME + ".PermissionTest FROM User_PermissionTest");
    }
}
