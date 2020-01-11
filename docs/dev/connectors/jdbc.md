---
title: "JDBC Connector"
nav-title: JDBC
nav-parent_id: connectors
nav-pos: 9
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}


---
title: "JDBC Connector"
nav-title: JDBC
nav-parent_id: connectors
nav-pos: 9
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}


This connector provides sinks that write data into a JDBC database.

To use this connector, add the following dependency to your project (along with your JDBC-driver):

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-jdbc{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently __NOT__ part of the binary distribution. See how to link with them for cluster execution [here]({{ site.baseurl}}/dev/projectsetup/dependencies.html).

JDBC sink usage depends on the use case and requirements.

## At-least-once delivery
This is the default Flink behaviour.

Usage:
{% highlight java %}
JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                    .setDrivername(<driver class name>)
                    .setDBUrl(<jdbc url>)
                    .setUsername(<username>)
                    .setPassword(<password>)
                    .setQuery(<insert or update query>)
                    .finish()
    );
JDBCSinkFunction sink = new JDBCSinkFunction(outputFormat);
{% endhighlight %}

Please refer to the [API documentation]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/java/io/jdbc/JDBCOutputFormat.JDBCOutputFormatBuilder.html) for more details.

## Effectively exactly-once
In certain use cases exactly-once can be achieved by using upsert queries.

### Pre-requisites
1. Uniquely identifiable records
1. Database support for Upsert or equivalent

### Usage
{% highlight java %}
JDBCUpsertOutputFormat format = JDBCUpsertOutputFormat.builder()
        .setOptions(JDBCOptions.builder()
                .setDBUrl(<jdbc url>)
                .setTableName(<table name>)
                .build())
        .setFieldNames(<fieldNames>)
        .setKeyFields(<keyFields>)
        .build();
);
JDBCUpsertSinkFunction sink = new JDBCUpsertSinkFunction(outputFormat);
{% endhighlight %}
You should use a Flink-supported dialect by setting it explicitly with setDialect or implicitly in JDBC URL.

Please refer to the [API documentation]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/java/io/jdbc/JDBCUpsertOutputFormat.Builder.html) for more details.

### Drawbacks
1. Pre-requisites must be met
1. Index overhead

## Exactly once
[JdbcXaExactlyOnceSinkFunction]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/java/io/jdbc/JdbcXaExactlyOnceSinkFunction.html)
uses XA transactions to provide exactly once guarantees.

That is, if a checkpoint succeeds, all records sent as part of this checkpoint are committed

If a checkpoint fails then job is restarted, database transaction is rolled back, and records are sent again.

Each parallel subtask has it's own transactions, independent from other subtasks.

### Pre-requisites
Database driver support.
Most RDBMS vendors support XA and include necessary classes into their standard driver jars.

### Usage
{% highlight java %}
Supplier<XADataSource> dataSourceSupplier = ... // should be serializable
Function<MyPojo, Row> rowConverter = ... // should be serializable
int maxCommitAttempts = 5;
Optional<Integer> timeoutSec = Optional.of(3);

JdbcXaExactlyOnceSinkFunction sink = new JdbcXaExactlyOnceSinkFunction(outputFormat,
                                                                       rowConverter,
                                                                       dataSourceSupplier,
                                                                       timeoutSec,
                                                                       maxCommitAttempts);
{% endhighlight %}

Please refer to the [API documentation]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/java/io/jdbc/JdbcXaExactlyOnceSinkFunction.html) for more details.

### XID generation
By default xids are derived from:
1. checkpoint id
1. subtask index
1. 4 random bytes to provide uniqueness across other jobs and apps (generated at startup using SecureRandom)

If this doesn't suit your environment (i.e. xids can collide) you can provide your XidGenerator implementation.

### Transactions cleanup after failure.
By default Flink rolls back only transactions that are known to it, i.e. saved in state.

You can instruct it to rollback other prepared transactions by using recoveredAndRollback constructor parameter.

NOTE that this can have some undesired effects:
- interfere with other subtasks or applications (one subtask rolling back transactions prepared by the other one (and known to it))
- block when using with some non-MVCC databases, if there are ended-not-prepared transactions

### State size
In the common case state holds only the previous and current transactions. 
However, it may contain more data in the following cases:
1. when commit failures accumulate. This can be controlled by the maxCommitAttempts parameter
1. after recovery from state with several not committed transactions
1. when MaxConcurrentCheckpoints is > 1

### Drawbacks
1. Pre-requisites must be met
1. transactions overhead (e.g. potential GC delay and increased undo segment)
