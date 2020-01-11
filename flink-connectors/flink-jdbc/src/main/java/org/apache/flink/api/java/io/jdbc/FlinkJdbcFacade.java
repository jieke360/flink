/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.io.jdbc.xa.JdbcXaSinkFunction;
import org.apache.flink.api.java.io.jdbc.xa.XaFacade;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import javax.sql.XADataSource;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * JDBC facade.
 */
@PublicEvolving
public final class FlinkJdbcFacade {
	private FlinkJdbcFacade() {
	}

	public static SinkFunction<Row> sink(JdbcConnectionOptions connectionOptions, JdbcInsertOptions insertOptions, JdbcBatchOptions batchOptions) {
		return new JDBCSinkFunction(new JDBCOutputFormat(new SimpleJdbcConnectionProvider(connectionOptions), insertOptions, batchOptions));
	}

	public static SinkFunction<Tuple2<Boolean, Row>> upsertSink(JdbcConnectionOptions connectionOptions, JdbcUpsertOptions updateOptions, JdbcBatchOptions jdbcBatchOptions) {
		return new JDBCUpsertSinkFunction(new JDBCUpsertOutputFormat(new SimpleJdbcConnectionProvider(connectionOptions), updateOptions, jdbcBatchOptions));
	}

	public static <T> SinkFunction<T> exactlyOnceSink(
			JdbcInsertOptions insertOptions,
			JdbcBatchOptions batchOptions,
			JdbcExactlyOnceOptions exactlyOnceOptions,
			Supplier<XADataSource> dataSourceSupplier,
			Function<T, Row> recordConverter) {
		return new JdbcXaSinkFunction<>(
				insertOptions,
				batchOptions,
				exactlyOnceOptions,
				XaFacade.fromXaDataSourceSupplier(dataSourceSupplier, Optional.empty()),
				recordConverter);
	}
}
