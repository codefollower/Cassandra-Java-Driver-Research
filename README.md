Cassandra-Java-Driver-Research
==================

Cassandra-Java-Driver源代码学习研究(包括代码注释、文档、用于代码分析的测试用例)

<<<<<<< HEAD

## 使用的Cassandra-Java-Driver版本

保持与官方的[trunk](https://github.com/datastax/java-driver.git)版本同步

=======
*If you're reading this on github.com, please note that this is the readme
for the development version and that some features described here might
not yet have been released. You can find the documentation for latest
version through [Java driver
docs](http://datastax.github.io/java-driver/) or via the release tags,
[e.g.
2.1.6](https://github.com/datastax/java-driver/tree/2.1.6).*

A modern, [feature-rich](features/) and highly tunable Java client
library for Apache Cassandra (1.2+) and DataStax Enterprise (3.1+) using
exclusively Cassandra's binary protocol and Cassandra Query Language v3.

**Features:**

* [Sync][sync] and [Async][async] API
* [Simple][simple_st], [Prepared][prepared_st], and [Batch][batch_st] statements
* Asynchronous IO, parallel execution, request pipelining
* [Connection pooling][pool]
* Auto node discovery
* Automatic reconnection
* Configurable [load balancing][lbp] and [retry policies][retry_policy]
* Works with any cluster size
* [Query builder][query_builder]
* [Object mapper][mapper]


[sync]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Session.html#execute(com.datastax.driver.core.Statement)
[async]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Session.html#executeAsync(com.datastax.driver.core.Statement)
[simple_st]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/SimpleStatement.html
[prepared_st]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/Session.html#prepare(com.datastax.driver.core.RegularStatement)
[batch_st]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/BatchStatement.html
[pool]: features/pooling/
[lbp]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[retry_policy]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/policies/RetryPolicy.html
[query_builder]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/core/querybuilder/QueryBuilder.html
[mapper]: http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/mapping/MappingManager.html 

The driver architecture is based on layers. At the bottom lies the driver core.
This core handles everything related to the connections to a Cassandra
cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple,
relatively low-level API on top of which higher level layers can be built.
>>>>>>> b4d397c33abec1f42fa8a35869e219ef60c0e7e8

## 构建与运行环境

需要JDK7以及[Apache Maven](http://maven.apache.org/)


<<<<<<< HEAD
## 安装
=======
**Community:**

- JIRA: https://datastax-oss.atlassian.net/browse/JAVA
- MAILING LIST: https://groups.google.com/a/lists.datastax.com/forum/#!forum/java-driver-user
- IRC: #datastax-drivers on [irc.freenode.net](http://freenode.net)
- TWITTER: Follow the latest news about DataStax Drivers - [@olim7t](http://twitter.com/olim7t), [@mfiguiere](http://twitter.com/mfiguiere)
- DOCS: http://www.datastax.com/documentation/developer/java-driver/2.1/index.html
- API: http://www.datastax.com/drivers/java/2.1
- CHANGELOG: https://github.com/datastax/java-driver/blob/2.1/driver-core/CHANGELOG.rst

## Maven
>>>>>>> b4d397c33abec1f42fa8a35869e219ef60c0e7e8

mvn install -Dmaven.test.skip=true

<<<<<<< HEAD
=======
```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>2.1.6</version>
</dependency>
```
>>>>>>> b4d397c33abec1f42fa8a35869e219ef60c0e7e8

## 生成Eclipse工程

<<<<<<< HEAD
mvn eclipse:eclipse <br><br>

在eclipse中导入 <br>
File->Import->General->Existing Projects into Workspace
=======
```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-mapping</artifactId>
  <version>2.1.6</version>
</dependency>
```

We also provide a [shaded JAR](features/shaded_jar/)
to avoid the explicit dependency to Netty.
>>>>>>> b4d397c33abec1f42fa8a35869e219ef60c0e7e8


## 运行测试例子

<<<<<<< HEAD
在eclipse中可以直接运行my.test包中的例子
=======
UDT and tuple support is available only when using Apache Cassandra 2.1 (see [CQL improvements in Cassandra 2.1](http://www.datastax.com/dev/blog/cql-in-2-1)).

Other features are available only when using Apache Cassandra 2.0 or higher (e.g. result set paging,
[BatchStatement](https://github.com/datastax/java-driver/blob/2.1/driver-core/src/main/java/com/datastax/driver/core/BatchStatement.java), 
[lightweight transactions](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_ltweight_transaction_t.html) 
-- see [What's new in Cassandra 2.0](http://www.datastax.com/documentation/cassandra/2.0/cassandra/features/features_key_c.html)). 
Trying to use these with a cluster running Cassandra 1.2 will result in 
an [UnsupportedFeatureException](https://github.com/datastax/java-driver/blob/2.1/driver-core/src/main/java/com/datastax/driver/core/exceptions/UnsupportedFeatureException.java) being thrown.


## Upgrading from previous versions

If you are upgrading from the 2.0.x branch of the driver, be sure to have a look at
the [upgrade guide](https://github.com/datastax/java-driver/blob/2.1/driver-core/Upgrade_guide_to_2.1.rst).

If you are upgrading from the 1.x branch, follow the [upgrade guide to 2.0](https://github.com/datastax/java-driver/blob/2.0/driver-core/Upgrade_guide_to_2.0.rst),
and then the above document.


### Troubleshooting

If you are having issues connecting to the cluster (seeing `NoHostAvailableConnection` exceptions) please check the 
[connection requirements](https://github.com/datastax/java-driver/wiki/Connection-requirements).


## License
Copyright 2012-2015, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
>>>>>>> b4d397c33abec1f42fa8a35869e219ef60c0e7e8
