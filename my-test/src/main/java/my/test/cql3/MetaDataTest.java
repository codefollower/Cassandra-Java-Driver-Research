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

public class MetaDataTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new MetaDataTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        cql = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name='mytest'";
        //printResultSet();

        cql = "SELECT * FROM system.schema_keyspaces";
        printResultSet();

        cql = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name='system'";
        //printResultSet();

        cql = "SELECT columnfamily_name FROM system.schema_columnfamilies";
        printResultSet();

        cql = "SELECT columnfamily_name,column_name FROM system.schema_columns";
        printResultSet();

        //        cql = "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name='system'";
        //        printResultSet();
        //        
        //        cql = "SELECT columnfamily_name,column_name FROM system.schema_columns WHERE keyspace_name='system'";
        //        printResultSet();
        //        
        //        cql = "SELECT columnfamily_name,column_name FROM system.schema_columns WHERE keyspace_name='mytest'";
        //        printResultSet();
    }

}
