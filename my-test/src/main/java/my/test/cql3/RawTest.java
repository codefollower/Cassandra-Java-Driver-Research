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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import my.test.TestBase;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;

public class RawTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new RawTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        // create();
        // testMarker();
        // testJson();
        // testTuple();
        testTypeCast();
    }

    void create() throws Exception {
        execute("CREATE TYPE IF NOT EXISTS fullname (firstname text, lastname text)");
        execute("DROP TABLE IF EXISTS users");
        execute("CREATE TABLE IF NOT EXISTS users " + //
                "(id int, f1 int, age int, f3 int, f4 int, f2 double, first_name text,last_name text," + //
                "f_set set<text>, f_list list<text>, f_map map<timestamp, text>, " + //
                "name frozen <fullname>, " + //
                // "f_static int static, " + //
                "f_tuple frozen<tuple<int, text, float>>, " + //
                // "PRIMARY KEY ((id,f1),age, f3, f4))");
                "PRIMARY KEY (id,f1))");
    }

    void testTypeCast() throws Exception {
        // 不能转
        cql = "insert into users(id, f1, f2) values (20, 21, (int)'22')";
        tryExecute(cql);

        cql = "insert into users(id, f1, f2) values (20, 21, (double)22)";
        execute(cql);

        // cql = "update users set f3=10 where id=10 if f1=10";
        // execute(cql);
    }

    void testJson() throws Exception {
        cql = "insert into users(id, f1, age) JSON '{id:1, f1:2, age:3}'"; // 不能加(id, f1, age)
        cql = "insert into users JSON '{\"id\":1, \"f1\":2, \"age\":3}'";
        execute(cql);

        cql = "insert into users JSON ?";
        PreparedStatement statement = prepare(cql);
        BoundStatement boundStatement = new BoundStatement(statement);
        boundStatement.setString(0, "{\"id\":1, \"f1\":2, \"age\":3}");
        execute(boundStatement);
    }

    // Tuple只能用于clustering columns
    void testTuple() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS users2 " + //
                "(id int, f1 int, f2 int, f3 int, " + //
                "f_tuple frozen<tuple<int, text, float>>, " + //
                // "PRIMARY KEY ((id,f1),age, f3, f4))");
                "PRIMARY KEY (id,f1,f2))");

        cql = "insert into users2(id, f1, f2, f_tuple) values (20, 21, 22, (20,'abc',0.1))";
        execute(cql);

        cql = "select f3 from users2 where id=20 and (f1,f2) = (20,21)";
        execute(cql);

        cql = "select f3 from users2 where id=20 and (f1,f2) in ((20,21),(22,23))";
        execute(cql);

        cql = "select f3 from users2 where id=20 and (f1,f2) in ?";
        PreparedStatement statement = prepare(cql);
        BoundStatement boundStatement = new BoundStatement(statement);
        TupleType tt;
        tt = cluster.getMetadata().newTupleType(DataType.cint(), DataType.cint());
        TupleValue tv = tt.newValue(20, 20);
        List<TupleValue> list = new ArrayList<TupleValue>(2);
        list.add(tv);
        list.add(tt.newValue(21, 21));
        ResultSet rs = execute(boundStatement.bind(list));
        tryPrintResultSet(rs);

        cql = "select f3 from users2 where id=20 and (f1,f2) = ?";
        statement = prepare(cql);
        boundStatement = new BoundStatement(statement);
        tt = cluster.getMetadata().newTupleType(DataType.cint(), DataType.cint());
        tv = tt.newValue(20, 20);
        boundStatement.bind(tv);
        rs = execute(boundStatement);
        tryPrintResultSet(rs);
    }

    void testMarker() throws Exception {
        cql = "insert into users(id, f1, age, f_set, f_map, f_list, name) values(?, ?, ?, ?, ?, ?, "
        // + "{firstname: 'a1', lastname: 'a2'})";
                + "{firstname: 'a1', lastname: ?})";

        PreparedStatement statement = prepare(cql);
        BoundStatement boundStatement = new BoundStatement(statement);
        boundStatement.setInt(0, 0);
        boundStatement.setInt(1, 1);
        boundStatement.setInt(2, 2);

        HashSet<String> set = new HashSet<String>();
        set.add("abc");

        HashMap<java.sql.Timestamp, String> map = new HashMap<java.sql.Timestamp, String>();
        map.put(new java.sql.Timestamp(new java.util.Date().getTime()), "abc");

        List<String> list = new ArrayList<String>(1);
        list.add("abc");
        list.add("123");

        boundStatement.setSet(3, set);
        boundStatement.setMap(4, map);
        boundStatement.setList(5, list);

        // HashMap<String, String> map2 = new HashMap<String, String>();
        // map2.put("first_name", "a1");
        // map2.put("last_name", "a2");
        // boundStatement.setMap(6, map2);

        boundStatement.setString(6, "a2");

        ResultSet rs = session.execute(boundStatement);
        tryPrintResultSet(rs);
    }
}
