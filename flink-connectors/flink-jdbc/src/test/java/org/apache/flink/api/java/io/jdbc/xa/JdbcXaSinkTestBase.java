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

package org.apache.flink.api.java.io.jdbc.xa;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JdbcBatchOptions;
import org.apache.flink.api.java.io.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.api.java.io.jdbc.JdbcInsertOptions;
import org.apache.flink.api.java.io.jdbc.JdbcTestBase;
import org.apache.flink.api.java.io.jdbc.JdbcTestFixture;
import org.apache.flink.api.java.io.jdbc.JdbcTestFixture.TestEntry;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;

import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.io.jdbc.JdbcTestFixture.INSERT_TEMPLATE;

/**
 * Base class for {@link JdbcXaSinkFunction} tests. In addition to {@link JdbcTestBase} init it initializes/closes helpers.
 */
public abstract class JdbcXaSinkTestBase extends JdbcTestBase {

	JdbcXaFacadeTestHelper xaHelper;
	JdbcXaSinkTestHelper sinkHelper;
	XADataSource xaDataSource;

	@Before
	public void initHelpers() throws Exception {
		xaDataSource = getDbMetadata().buildXaDataSource();
		xaHelper = new JdbcXaFacadeTestHelper(getDbMetadata().buildXaDataSource(), getDbMetadata().getUrl(), JdbcTestFixture.INPUT_TABLE);
		sinkHelper = buildSinkHelper(createStateHandler());
	}

	protected XaSinkStateHandler createStateHandler() {
		return new TestXaSinkStateHandler();
	}

	@After
	public void closeHelpers() throws Exception {
		if (sinkHelper != null) {
			sinkHelper.close();
		}
		if (xaHelper != null) {
			xaHelper.close();
		}
		try (JdbcXaFacadeTestHelper xa = new JdbcXaFacadeTestHelper(xaDataSource, getDbMetadata().getUrl(), JdbcTestFixture.INPUT_TABLE)) {
			xa.cancelAllTx();
		}
	}

	JdbcXaSinkTestHelper buildSinkHelper(XaSinkStateHandler stateHandler) throws Exception {
		return new JdbcXaSinkTestHelper(buildAndInit(0, getXaFacade(), stateHandler), stateHandler);
	}

	protected XaFacadeImpl getXaFacade() {
		return XaFacadeImpl.fromXaDataSource(xaDataSource);
	}

	JdbcXaSinkFunction<TestEntry> buildAndInit() throws Exception {
		return buildAndInit(Integer.MAX_VALUE, getXaFacade());
	}

	JdbcXaSinkFunction<TestEntry> buildAndInit(int batchInterval, XaFacade xaFacade) throws Exception {
		return buildAndInit(batchInterval, xaFacade, createStateHandler());
	}

	static JdbcXaSinkFunction<TestEntry> buildAndInit(int batchInterval, XaFacade xaFacade, XaSinkStateHandler state) throws Exception {
		JdbcXaSinkFunction<TestEntry> sink = buildSink(new SemanticXidGenerator(), xaFacade, state, batchInterval);
		sink.initializeState(buildInitCtx(false));
		sink.open(new Configuration());
		return sink;
	}

	static JdbcXaSinkFunction<TestEntry> buildSink(XidGenerator xidGenerator, XaFacade xaFacade, XaSinkStateHandler state, int batchInterval) {
		JDBCOutputFormat format = new JDBCOutputFormat(xaFacade,
				JdbcInsertOptions.builder().withQuery(String.format(INSERT_TEMPLATE, JdbcTestFixture.INPUT_TABLE)).build(),
				JdbcBatchOptions.builder().withSize(batchInterval).build());
		JdbcXaSinkFunction<TestEntry> sink = new JdbcXaSinkFunction<>(format, ENTRY_TO_ROW_CONVERTER, xaFacade, xidGenerator, state, JdbcExactlyOnceOptions.builder().withRecoveredAndRollback(true).build());
		sink.setRuntimeContext(TEST_RUNTIME_CONTEXT);
		return sink;
	}

	static final RuntimeContext TEST_RUNTIME_CONTEXT = new RuntimeContext() {
		@Override
		public String getTaskName() {
			return "test";
		}

		@Override
		public MetricGroup getMetricGroup() {
			return null;
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return 1;
		}

		@Override
		public int getMaxNumberOfParallelSubtasks() {
			return 1;
		}

		@Override
		public int getIndexOfThisSubtask() {
			return 0;
		}

		@Override
		public int getAttemptNumber() {
			return 0;
		}

		@Override
		public String getTaskNameWithSubtasks() {
			return "test";
		}

		@Override
		public ExecutionConfig getExecutionConfig() {
			return null;
		}

		@Override
		public ClassLoader getUserCodeClassLoader() {
			return null;
		}

		@Override
		public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {

		}

		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
			return null;
		}

		@Override
		public Map<String, Accumulator<?, ?>> getAllAccumulators() {
			return null;
		}

		@Override
		public IntCounter getIntCounter(String name) {
			return null;
		}

		@Override
		public LongCounter getLongCounter(String name) {
			return null;
		}

		@Override
		public DoubleCounter getDoubleCounter(String name) {
			return null;
		}

		@Override
		public Histogram getHistogram(String name) {
			return null;
		}

		@Override
		public boolean hasBroadcastVariable(String name) {
			return false;
		}

		@Override
		public <RT> List<RT> getBroadcastVariable(String name) {
			return null;
		}

		@Override
		public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
			return null;
		}

		@Override
		public DistributedCache getDistributedCache() {
			return null;
		}

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			return null;
		}

		@Override
		public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
			return null;
		}

		@Override
		@SuppressWarnings("deprecation")
		public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
			return null;
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			return null;
		}
	};


	@SuppressWarnings("rawtypes")
	static final SinkFunction.Context TEST_SINK_CONTEXT = new SinkFunction.Context() {
		@Override
		public long currentProcessingTime() {
			return 0;
		}

		@Override
		public long currentWatermark() {
			return 0;
		}

		@Override
		public Long timestamp() {
			return 0L;
		}
	};

	static final Function<TestEntry, Row> ENTRY_TO_ROW_CONVERTER = new TestEntryRowMapper();

	static class TestXaSinkStateHandler implements XaSinkStateHandler {
		private JdbcXaSinkFunctionState stored;

		@Override
		public JdbcXaSinkFunctionState load(FunctionInitializationContext context) {
			List<CheckpointAndXid> prepared =
					stored != null ? stored.getPrepared().stream().map(CheckpointAndXid::asRestored).collect(Collectors.toList()) : Collections.emptyList();
			Collection<Xid> hanging = stored != null ? stored.getHanging() : Collections.emptyList();
			return JdbcXaSinkFunctionState.of(prepared, hanging);
		}

		@Override
		public void store(JdbcXaSinkFunctionState state) {
			stored = state;
		}

		JdbcXaSinkFunctionState get() {
			return stored;
		}

		@Override
		public String toString() {
			return stored == null ? null : stored.toString();
		}
	}

	static StateInitializationContextImpl buildInitCtx(boolean restored) {
		return new StateInitializationContextImpl(
				restored,
				new DefaultOperatorStateBackend(
						new ExecutionConfig(),
						new CloseableRegistry(),
						new HashMap<>(),
						new HashMap<>(),
						new HashMap<>(),
						new HashMap<>(),
						null),
				null,
				null,
				null);
	}

	private static class TestEntryRowMapper implements Function<TestEntry, Row>, Serializable {
		@Override
		public Row apply(TestEntry entry) {
			Row row = new Row(5);
			row.setField(0, entry.id);
			row.setField(1, entry.title);
			row.setField(2, entry.author);
			row.setField(3, entry.price);
			row.setField(4, entry.qty);
			return row;
		}
	}
}
