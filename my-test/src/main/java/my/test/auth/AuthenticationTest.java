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
package my.test.auth;

import my.test.TestBase;

public class AuthenticationTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new AuthenticationTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        testCreateRoleStatement();
        execute("LIST USERS");
        
        testAlterRoleStatement();
        testDropRoleStatement();
    }

    void testCreateRoleStatement() {
        execute("CREATE USER AuthenticationTest WITH PASSWORD 'mypassword' SUPERUSER");
    }

    void testAlterRoleStatement() {
        execute("ALTER USER AuthenticationTest WITH PASSWORD 'mypassword2' SUPERUSER");
    }

    void testDropRoleStatement() {
        execute("DROP USER AuthenticationTest");
    }
}
