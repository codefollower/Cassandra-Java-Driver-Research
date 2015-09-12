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
import java.util.List;

import my.test.TestBase;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;

public class SelectTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new SelectTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        tableName = "SelectTest";
        create();
        insert();
        // select();
        // test_execute();
        // test();

        // test_getSliceCommands();
        // test_getKeyBounds();
    }

    void test_execute() {
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20";
        // tryPrintResultSet();

        test_NamesQueryFilter();
    }

    void test_NamesQueryFilter() {

        // execute("DROP TABLE IF EXISTS users2");
        // execute("CREATE TABLE IF NOT EXISTS users2 " + //
        // "(id int, f1 int, f2 int, age int, " + //
        // "PRIMARY KEY (id, f1, f2)) WITH COMPACT STORAGE");
        //
        // execute("insert into users2(id, f1, f2, age) values(1, 2, 3, 4)");
        //
        // cql = "select * from users2 where id = 1 and f1 = 2 and f2 = 3";
        // tryPrintResultSet();

        // execute("DROP TABLE IF EXISTS users3");
        // execute("CREATE TABLE IF NOT EXISTS users3 " + //
        // "(id int, f1 int, f2 int, age int, " + //
        // "PRIMARY KEY (id)) WITH COMPACT STORAGE");
        //
        // execute("insert into users3(id, f1, f2, age) values(1, 2, 3, 4)");

        // cql = "select * from users3 where id = 1 and f1 = 2 and f2 = 3";
        // //cql = "select * from users3 where id = 1 and f1 = 2 and f2 = 3 ALLOW FILTERING";
        // cql = "select f1, f2 from users3 where id = 1";
        // tryPrintResultSet();

        // execute("DROP TABLE IF EXISTS users4");
        // execute("CREATE TABLE IF NOT EXISTS users4 " + //
        // "(id int, f1 int, f2 int, age int, " + //
        // "PRIMARY KEY (id))");
        //
        execute("insert into users4(id, f1, f2, age) values(1, 2, 3, 4)");

        // cql = "CREATE INDEX IF NOT EXISTS users4_f1 ON users4 (f1)";
        // session.execute(cql);

        // cql = "select * from users4 where id = 1 and f1 = 2 and f2 = 3";
        // cql = "select * from users4 where id = 1 and f1 = 2 and f2 = 3 ALLOW FILTERING";
        // cql = "select * from users4 where id = 1";
        //
        // cql = "select * from users4 where id = 1 and f1 = 2";
        // cql = "select * from users4 where id = 1 and f1 = 2 ALLOW FILTERING";
        // cql = "select * from users4 where id = 1";
        // tryPrintResultSet();

        // execute("DROP TABLE IF EXISTS users5");
        // execute("CREATE TABLE IF NOT EXISTS users5 " + //
        // "(id int, f1 int, f2 int, age int, " + //
        // "PRIMARY KEY (id, f1, f2)) WITH COMPACT STORAGE");
        //
        // execute("insert into users5(id, f1, f2, age) values(1, 2, 3, 4)");

        cql = "select * from users5 where id = 1 and f1 = 2";
        cql = "select * from users5 where id = 1 and f1 = 2 and f2 = 3";
        // cql = "select * from users4 where id = 1 and f1 = 2 ALLOW FILTERING";
        // cql = "select * from users4 where id = 1";
        // tryPrintResultSet();

        // execute("DROP TABLE IF EXISTS users6");
        // execute("CREATE TABLE IF NOT EXISTS users6 " + //
        // "(id int, f1 int, f2 int, " + //
        // "PRIMARY KEY (id, f1)) WITH COMPACT STORAGE");
        //
        // execute("insert into users6(id, f1, f2) values(1, 2, 3)");

        cql = "select * from users6 where id = 1"; // isColumnRange()返回true，因为缺少了f1
        cql = "select * from users6 where id = 1 and f1 = 2"; // isColumnRange()返回false
        // cql = "select * from users6 where id = 1 and f1 > 2"; //isColumnRange()返回true，因为f1用了范围查询
        // tryPrintResultSet();

        // execute("DROP TABLE IF EXISTS users7");
        // execute("CREATE TABLE IF NOT EXISTS users7 " + //
        // "(id int, f1 int, f2 int, age int, " + //
        // "PRIMARY KEY (id, f1, f2))");

        // execute("insert into users7(id, f1, f2, age) values(1, 2, 3, 4)");

    }

    void create() {
        execute("CREATE TYPE IF NOT EXISTS fullname (firstname text, lastname text)");
        session.execute("DROP TABLE IF EXISTS users");
        session.execute("CREATE TABLE IF NOT EXISTS users " + //
                "(id int, f1 int, age int, f3 int, f4 int, f2 double, first_name text,last_name text," + //
                "emails set<text>, top_places list<text>, todo map<timestamp, text>, " + //
                "name frozen <fullname>, " + //
                "f_static int static, " + //
                "f_tuple frozen<tuple<int, text, float>>, " + //
                "PRIMARY KEY ((id,f1),age, f3, f4))");
        cql = "CREATE INDEX IF NOT EXISTS users_age ON users (age)";
        // session.execute(cql);

        cql = "CREATE INDEX IF NOT EXISTS users_first_name ON users (first_name)";
        // session.execute(cql);

        // execute("CREATE FUNCTION IF NOT EXISTS my::sin ( input double ) " + //
        // "RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");

        execute("CREATE FUNCTION IF NOT EXISTS sin ( input double ) CALLED ON NULL INPUT " + //
                "RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'");
    }

    void test_Restriction() {
        cql = "select * from users WHERE f1=20 and f3<30 and f4<=40";
        tryPrintResultSet();

        cql = "select * from users WHERE f1=20 and f4<30 and f3<=40";
        tryPrintResultSet();

        cql = "select * from users WHERE f1=20 and f3=30 and f4<=40";
        tryPrintResultSet();

        cql = "select * from users WHERE id=20 and f3=30 and f4<=40";
        tryPrintResultSet();

        cql = "select * from users WHERE id=20 and f1=20 and f3=20 and f4<=40";
        tryPrintResultSet();

        cql = "select * from users WHERE id=20 and f1=20 and age=20 and f3=20 and f4<=40";
        tryPrintResultSet();
    }

    void test_MultiColumnRelation() {
        cql = "select age, f2, f3 from users where id=20 and f1=20 and (age, f3) >= ?";
        cql = "select age, f2, f3 from users where (age, f3) >= ? ALLOW FILTERING";
        PreparedStatement statement = session.prepare(cql);
        BoundStatement boundStatement = new BoundStatement(statement);
        TupleType tt = cluster.getMetadata().newTupleType(DataType.cint(), DataType.cint());
        // TupleType tt = TupleType.of(DataType.cint(), DataType.cint());
        TupleValue tv = tt.newValue(20, 20);
        ResultSet rs = session.execute(boundStatement.bind(tv));
        tryPrintResultSet(rs);

        cql = "select age, f2, f3 from users where id=20 and f1=20 and (age, f3) in ?";
        statement = session.prepare(cql);
        boundStatement = new BoundStatement(statement);

        // tt = TupleType.of(DataType.cint(), DataType.cint());
        tt = cluster.getMetadata().newTupleType(DataType.cint(), DataType.cint());
        // DataType dt = DataType.list(tt);
        tv = tt.newValue(20, 20);
        List<TupleValue> list = new ArrayList<TupleValue>(2);
        list.add(tv);
        list.add(tt.newValue(21, 21));
        rs = session.execute(boundStatement.bind(list));
        tryPrintResultSet(rs);

        cql = "select age, f2, f3 from users where id=20 and f1=20 and (age, f3) >= (20, 20)";
        cql = "select age, f2, f3 from users";
        tryPrintResultSet();

        cql = "select age, f2, f3 from users where id=20 and f1=20 and (age, f3) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Multi-column relations can only be applied to clustering columns:
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (f1, age) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Column "age" appeared twice in a relation: (age, age) IN ((20, 20), (21, 21))
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, age) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order.
        // Got (f3, age) IN ((20, 20), (21, 21))
        // 必需按PRIMARY KEY ((id,f1),age, f3))
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (f3, age) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Clustering columns must appear in the PRIMARY KEY order in multi-column relations: (age, f4) IN ((20, 20),
        // (21, 21))
        // 少了f3
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, f4) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Column "age" cannot be restricted by more than one relation if it is in an IN relation
        // 前面又有一个age = 20
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and age = 20 and (age, f3) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Column "age" cannot be restricted by more than one relation if it is in an = relation
        // 前面又有一个age in (20, 21)
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and age in (20, 21) and(age, f3) = (20, 20)";
        tryPrintResultSet();

        // Column "age" cannot be restricted by an equality relation and an inequality relation
        // 后面是一个>，前面是一个=，这样是不允许的
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and age = 20 and(age, f3) > (20, 20)";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, f3) in ((20, 20), (21, 21))";
        printResultSet();

        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, f3) > (20, 20)";
        printResultSet();
    }

    void select() {
        // test_Restriction();
        // test_MultiColumnRelation();
        select1();
    }

    void select1() {
        // Only EQ and IN relation are supported on the partition key (unless you use the token() function)
        cql = "select * " + //
                "from users where id>=20 and f1=20";
        tryPrintResultSet();

        cql = "select age, WRITETIME(f_tuple), TTL(first_name), name.firstname, now(), " + KEYSPACE_NAME + ".sin(f2) " + //
                "from users where id=20 and f1=20";
        printResultSet();

        cql = "select * " + //
                "from users where id=20 and f1=20";
        printResultSet();

        cql = "select * from users WHERE token(id,f1) > 10";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where id=20 and f1=20";
        printResultSet();

        cql = "select age, WRITETIME(f_tuple), TTL(first_name), name.firstname, now(), my::sin(f2) " + //
                "from users where id=20 and f1=20";
        printResultSet();

        // 下面的代码测试org.apache.cassandra.cql3.statements.SelectStatement.RawStatement.updateRestrictionsForRelation
        // Multi-column的场景

        // Multi-column relations can only be applied to clustering columns:
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (f1, age) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Column "age" appeared twice in a relation: (age, age) IN ((20, 20), (21, 21))
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, age) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order.
        // Got (f3, age) IN ((20, 20), (21, 21))
        // 必需按PRIMARY KEY ((id,f1),age, f3))
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (f3, age) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Clustering columns must appear in the PRIMARY KEY order in multi-column relations: (age, f4) IN ((20, 20),
        // (21, 21))
        // 少了f3
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, f4) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Column "age" cannot be restricted by more than one relation if it is in an IN relation
        // 前面又有一个age = 20
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and age = 20 and (age, f3) in ((20, 20), (21, 21))";
        tryPrintResultSet();

        // Column "age" cannot be restricted by more than one relation if it is in an = relation
        // 前面又有一个age in (20, 21)
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and age in (20, 21) and(age, f3) = (20, 20)";
        tryPrintResultSet();

        // Column "age" cannot be restricted by an equality relation and an inequality relation
        // 后面是一个>，前面是一个=，这样是不允许的
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and age = 20 and(age, f3) > (20, 20)";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, f3) in ((20, 20), (21, 21))";
        printResultSet();

        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and (age, f3) > (20, 20)";
        printResultSet();

        //
        // 下面的代码测试org.apache.cassandra.cql3.statements.SelectStatement.RawStatement.updateSingleColumnRestriction

        // The token() function is only supported on the partition key, found on age
        // token()只能用于partition key，连clustering column都不行
        cql = "select age, f2 " + //
                "from users where token(age) > 10";
        tryPrintResultSet();

        // Collection column 'emails' (set<text>) cannot be restricted by a '>' relation
        // Collection类型的字段只能使用CONTAINS和CONTAINS_KEY运算符
        cql = "select age, f2 " + //
                "from users where emails > 10";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and emails CONTAINS 'f@baggins.com'";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and f2 in (2.5)"; // 非PK列可以用in，但是里面只能有一个值
        tryPrintResultSet();

        // IN predicates on non-primary-key columns (f2) is not yet supported
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and f2 in (2.5, 3.5)";
        tryPrintResultSet();

        // Mixing single column relations and multi column relations on clustering columns is not allowed
        // clustering column不能同时存在SingleColumnRelations(f4 = 20)和MultiColumnRelations((age, f3) > (20, 20))
        cql = "select age, f2 " + //
                "from users where id=20 and f1=20 and f4 = 20 and (age, f3) > (20, 20)";
        tryPrintResultSet();

        //
        // 下面的代码测试org.apache.cassandra.cql3.statements.SelectStatement.RawStatement.processPartitionKeyRestrictions

        // The token() function must be applied to all partition key components or none of them
        cql = "select age, f2 " + //
                "from users where token(id) > 10";
        tryPrintResultSet();

        // Only EQ and IN relation are supported on the partition key (unless you use the token() function)
        cql = "select age, f2 " + //
                "from users where id > 10";
        tryPrintResultSet();

        // Partition key part f1 must be restricted since preceding part is
        cql = "select age, f2 " + //
                "from users where id = 10";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where id = 10 and age=1";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where f1 > 10";
        tryPrintResultSet();

        // Partitioning column "f1" cannot be restricted because the preceding column ("id") is either not restricted or
        // is restricted by a non-EQ relation
        cql = "select age, f2 " + //
                "from users where f1 = 10";
        tryPrintResultSet();

        // Partition KEY part id cannot be restricted by IN relation (only the last part of the partition key can)
        cql = "select age, f2 " + //
                "from users where id in (10)";
        tryPrintResultSet();

        // The token function arguments must be in the partition key order: ColumnDefinition{name=id,
        // type=org.apache.cassandra.db.marshal.Int32Type, kind=PARTITION_KEY, componentIndex=0, indexName=null,
        // indexType=null},ColumnDefinition{name=f1, type=org.apache.cassandra.db.marshal.Int32Type, kind=PARTITION_KEY,
        // componentIndex=1, indexName=null, indexType=null}
        cql = "select age, f2 " + //
                "from users where token(f1) > 10 and token(id) < 10";
        tryPrintResultSet();

        // Cannot restrict clustering columns when selecting only static columns
        cql = "select f_static " + //
                "from users where age = 10";
        tryPrintResultSet();

        //
        // 下面的代码测试org.apache.cassandra.cql3.statements.SelectStatement.RawStatement.processColumnRestrictions

        // PRIMARY KEY column "f3" cannot be restricted (preceding column "ColumnDefinition{name=age}" is either not
        // restricted or by a non-EQ relation)
        cql = "select age, f2 " + //
                "from users where f3 = 10";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where f3 = 10 and first_name = 'a'";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where age > 0";
        tryPrintResultSet();

        // execute("DROP TABLE IF EXISTS users2");
        // execute("CREATE TABLE IF NOT EXISTS users2 " + //
        // "(id int, f1 int, age int, " + //
        // "PRIMARY KEY (id, f1)) WITH COMPACT STORAGE");
        //
        // cql = "select age, f1 " + //
        // "from users2 where f1 > 0";
        // tryPrintResultSet();

        // Cannot restrict column "age" by IN relation as a collection is selected by the query
        cql = "select age, f2, emails " + //
                "from users where age in(20)";
        tryPrintResultSet();

        // No indexed columns present in by-columns clause with Equal operator
        cql = "select age, f2, emails " + //
                "from users where f2 = 1.5";
        tryPrintResultSet();

        //
        // 下面的代码测试org.apache.cassandra.cql3.statements.SelectStatement.RawStatement.validateSecondaryIndexSelections

        // Select on indexed columns and with IN clause for the PRIMARY KEY are not supported
        cql = "select age, f2 " + //
                "from users where id = 20 and f1 in(20, 21) and age = 20 and f2 = 1.5";
        tryPrintResultSet();

        // Queries using 2ndary indexes don't support selecting only static columns
        cql = "select f_static " + //
                "from users where first_name = 'a'";
        tryPrintResultSet();

        //
        // 下面的代码测试org.apache.cassandra.cql3.statements.SelectStatement.RawStatement.processOrderingClause

        // ORDER BY with 2ndary indexes is not supported.
        cql = "select age, f2 " + //
                "from users where first_name = 'a' order by f2";
        tryPrintResultSet();

        // ORDER BY is only supported when the partition key is restricted by an EQ or an IN.
        cql = "select age, f2 " + //
                "from users order by f2";
        tryPrintResultSet();

        // Order by is currently only supported on the clustered columns of the PRIMARY KEY, got f2
        cql = "select age, f2 " + //
                "from users where id = 20 and f1 in(20, 21) order by f2, emails";
        tryPrintResultSet();

        // Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY
        cql = "select age, f2 " + //
                "from users where id = 20 and f1 in(20, 21) order by f3, age";
        tryPrintResultSet();

        // Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY
        cql = "select age, f2 " + //
                "from users where id = 20 and f1 in(20, 21) order by age, f4";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users where id = 20 and f1 in(20, 21) order by age, f3";
        tryPrintResultSet();

        // Unsupported order by relation
        // age和f3必需同时是ASC或DESC
        cql = "select age, f2 " + //
                "from users where id = 20 and f1 in(20, 21) order by age, f3 DESC";
        tryPrintResultSet();

        //
        // 下面的代码测试org.apache.cassandra.cql3.statements.SelectStatement.RawStatement.checkNeedsFiltering

        // Cannot execute this query as it might involve data filtering and thus may have unpredictable performance.
        // If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING
        cql = "select age, f2 " + //
                "from users where first_name = 'a' and f2=1.5";
        tryPrintResultSet();

        cql = "select age, f2 " + //
                "from users";
        tryPrintResultSet();
    }

    void insert() {

        // session.execute("INSERT INTO users (id, f1, age, first_name, last_name, emails) "
        // + "VALUES('frodo', 11, 20, 'Frodo', 'Baggins', {'f@baggins.com','baggins@gmail.com'});");
        // session.execute("INSERT INTO users (id, f1, age, first_name, last_name, emails) "
        // + "VALUES('frodo', 12, 10, 'Frodo', 'Baggins', {'f@baggins.com','baggins@gmail.com'});");
        // session.execute("INSERT INTO users (id, f1, age, first_name, last_name, emails) "
        // + "VALUES('frodo', 13, 30, 'Frodo', 'Baggins', {'f@baggins.com','baggins@gmail.com'});");

        for (int i = 100; i < 200; i++)
            session.execute("INSERT INTO users " + //
                    "(id, f1, age, f3, f4, f_static, f2, name, emails, f_tuple) " + //
                    "VALUES(" + i + ", " + i + ", " + i + ", " + i + ", " + i + ", " + i + ", 3.5, " + //
                    "{firstname: 'Marie-Claude',lastname: 'Josset'}," + //
                    "{'f@baggins.com','baggins@gmail.com'}, " + //
                    "(3, 'bar', 2.1)" + //
                    ")");

    }

    void test_getSliceCommands() throws Exception {
        cql = "SELECT * FROM users WHERE id = 'frodo' AND f1 = 11";

        // 只有最后一个才允许在in中指定多个值
        cql = "SELECT * FROM users WHERE id in ('frodo','2')";
        cql = "SELECT * FROM users WHERE id in ('frodo')";

        cql = "SELECT * FROM users WHERE id in ('frodo') AND f1 = 11";

        cql = "SELECT * FROM users WHERE id = 'frodo' AND f1 in(11, 12)";

        cql = "SELECT * FROM users WHERE id = 'frodo' AND f1 in()";
        printResultSet();
    }

    void test_getKeyBounds() throws Exception {
        cql = "SELECT * FROM users WHERE id = 'frodo' AND f1 = 11";

        // 只有最后一个才允许在in中指定多个值
        cql = "SELECT * FROM users WHERE id in ('frodo','2')";
        cql = "SELECT * FROM users WHERE id in ('frodo')";

        cql = "SELECT * FROM users WHERE id in ('frodo') AND f1 = 11";

        cql = "SELECT * FROM users WHERE id = 'frodo' AND f1 in(11, 12)";

        cql = "SELECT * FROM users WHERE token(id) >= 22 AND token(f1) = 11";

        // 不能这样用，得加索引字段
        cql = "SELECT * FROM users WHERE id = 'frodo'";

        cql = "SELECT * FROM users WHERE id = 'frodo' AND age = 20 ALLOW FILTERING";

        cql = "SELECT * FROM users WHERE id = 'frodo' AND f1 in(11, 12)";
        printResultSet();
    }

    public void test() {

        cql = "SELECT * FROM users WHERE id = 'frodo'";
        // 错误:Cannot use selection function writeTime on PRIMARY KEY part id
        cql = "SELECT WRITETIME(id), TTL(emails) FROM users WHERE id = 'frodo'";
        // 错误:Cannot use selection function ttl on collections
        cql = "SELECT WRITETIME(first_name), TTL(emails) FROM users WHERE id = 'frodo'";
        cql = "SELECT id, emails, WRITETIME(first_name), TTL(last_name),token(id) FROM users WHERE id = 'frodo'";
        cql = "SELECT id, age, emails, WRITETIME(first_name), TTL(last_name),token(id) FROM users "
                + "WHERE id = 'frodo' ORDER BY age DESC";

        cql = "SELECT count(*) as c FROM users WHERE id = 'frodo'";
        cql = "SELECT DISTINCT first_name FROM users WHERE id = 'frodo'";
        cql = "SELECT * FROM users WHERE id = 'frodo' LIMIT 2";

        // 错误: Aliases aren't allowed in where clause ('a EQ 10')
        cql = "SELECT age as a FROM users WHERE id = 'frodo' and a=10";
        // 错误: Undefined name undefined in where clause ('undefined EQ 10')
        cql = "SELECT age as a FROM users WHERE id = 'frodo' and undefined=10";

        cql = "SELECT * FROM users WHERE id = 'frodo' and age=10";
        // 错误: id cannot be restricted by more than one relation if it includes an Equal
        cql = "SELECT * FROM users WHERE id = 'frodo' and age=10 and id = 'frodo2'";
        cql = "SELECT * FROM users WHERE id in('frodo','frodo2') and age=10";

        cql = "SELECT * FROM users WHERE id in('frodo','frodo2') and age=10 and last_name>'a'";

        // 错误: IN predicates on non-primary-key columns (last_name) is not yet supported
        cql = "SELECT * FROM users WHERE id = 'frodo' and last_name in('frodo','frodo2')";
        // in只支持一个值
        cql = "SELECT * FROM users WHERE id = 'frodo' and last_name in('frodo')";

        cql = "SELECT token(id) FROM users WHERE age=10";
        cql = "SELECT token(id, f1) FROM users WHERE id = 'frodo' and age=10";

        cql = "SELECT * FROM users WHERE age>=10 ALLOW FILTERING";
        cql = "SELECT * FROM users WHERE id = 'frodo' and f1=11 and age>=10 ALLOW FILTERING";

        // cql = "SELECT * FROM users WHERE token(id,f1) > 10";

        // cql = "SELECT token(id),token(20) FROM users WHERE age>=10 ALLOW FILTERING";

        // where子句只支持and不支持or
        // cql = "SELECT * FROM users WHERE id = 'frodo' or f1=11 and age>=10 ALLOW FILTERING";

        cql = "SELECT id FROM users WHERE age>=10 ALLOW FILTERING";

        // 少了as
        cql = "SELECT id uid FROM users WHERE age>=10 ALLOW FILTERING";
        cql = "SELECT id as uid FROM users WHERE age>=10 ALLOW FILTERING";

        cql = "SELECT id.a FROM users WHERE age>=10 ALLOW FILTERING";

        cql = "SELECT * FROM users WHERE age=10";

        cql = "SELECT * FROM users WHERE age=10 ORDER BY age DESC";

        cql = "SELECT * FROM users WHERE age>=10 LIMIT 2 ALLOW FILTERING";

        cql = "SELECT DISTINCT id FROM users WHERE id = 'frodo' AND f1 = 11 ORDER BY age DESC LIMIT 2 ALLOW FILTERING";

        SimpleStatement stmt = newSimpleStatement(cql);
        stmt.setFetchSize(20);
        ResultSet results = session.execute(stmt);
        printResultSet(results);
    }
}
