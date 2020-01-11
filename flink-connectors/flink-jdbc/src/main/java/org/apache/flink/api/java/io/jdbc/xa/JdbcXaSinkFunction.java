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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JdbcBatchOptions;
import org.apache.flink.api.java.io.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.api.java.io.jdbc.JdbcInsertOptions;
import org.apache.flink.api.java.io.jdbc.xa.XaFacade.EmptyXaTransactionException;
import org.apache.flink.api.java.io.jdbc.xa.XaFacade.TransientXaException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.io.jdbc.xa.JdbcXaSinkFunctionState.of;

/**
 * JDBC sink that uses XA transactions to provide exactly once guarantee. That is, if a checkpoint succeeds then all records
 * sent by {@link JdbcXaSinkFunction} instance to an external system are committed (and rolled back if it fails).
 * Each parallel subtask has it's own transactions, independent from other subtasks.
 */
@Internal
public class JdbcXaSinkFunction<T> extends AbstractRichFunction implements CheckpointedFunction, CheckpointListener, SinkFunction<T>, AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcXaSinkFunction.class);

	private final XaFacade xaFacade;
	private final XidGenerator xidGenerator;
	private final JDBCOutputFormat format;
	private final Function<T, Row> converter;
	private final XaSinkStateHandler stateHandler;
	private final JdbcExactlyOnceOptions options;

	// checkpoints and the corresponding transactions waiting for completion notification from JM
	private transient List<CheckpointAndXid> preparedXids = new ArrayList<>();
	// hanging XIDs - used for cleanup
	// it's a list to support retries and scaling down
	// possible transaction states: active, idle, prepared
	// last element is the current xid
	private transient Deque<Xid> hangingXids = new LinkedList<>();
	private transient Xid currentXid;

	/**
	 * Creates a {@link JdbcXaSinkFunction} using {@link XaSinkStateHandlerImpl} with {@link XaSinkStateSerializer}
	 * and {@link XidGenerator#semanticXidGenerator() semantic xid generator}.
	 * <p>All parameters must be {@link java.io.Serializable serializable}.</p>
	 */
	public JdbcXaSinkFunction(JdbcInsertOptions insertOptions, JdbcBatchOptions batchOptions, JdbcExactlyOnceOptions exactlyOnceOptions, XaFacade xaFacade, Function<T, Row> converter) {
		this(
				new JDBCOutputFormat(xaFacade, insertOptions, batchOptions),
				converter,
				xaFacade,
				XidGenerator.semanticXidGenerator(),
				new XaSinkStateHandlerImpl(new XaSinkStateSerializer()),
				exactlyOnceOptions
		);
	}

	/**
	 * Creates a {@link JdbcXaSinkFunction}.
	 * <p>All parameters must be {@link java.io.Serializable serializable}.</p>
	 *
	 * @param format       {@link JDBCOutputFormat} to write records with
	 * @param converter    a function to convert records to {@link Row rows}
	 * @param xaFacade     {@link XaFacade} to manage XA transactions
	 * @param xidGenerator {@link XidGenerator} to generate new transaction ids
	 */
	JdbcXaSinkFunction(
			JDBCOutputFormat format,
			Function<T, Row> converter,
			XaFacade xaFacade,
			XidGenerator xidGenerator,
			XaSinkStateHandler stateHandler,
			JdbcExactlyOnceOptions options) {
		this.xaFacade = Preconditions.checkNotNull(xaFacade);
		this.xidGenerator = Preconditions.checkNotNull(xidGenerator);
		this.format = Preconditions.checkNotNull(format);
		this.converter = Preconditions.checkNotNull(converter);
		this.stateHandler = Preconditions.checkNotNull(stateHandler);
		this.options = Preconditions.checkNotNull(options);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		JdbcXaSinkFunctionState state = stateHandler.load(context);
		hangingXids = new LinkedList<>(state.getHanging());
		preparedXids = new ArrayList<>(state.getPrepared());
		LOG.info("initialized state: prepared xids: {}, hanging xids: {}", preparedXids.size(), hangingXids.size());
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		xidGenerator.open();
		xaFacade.open();
		format.open(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
		hangingXids = new LinkedList<>(failOrRollback(hangingXids));
		commitUpToCheckpoint(Optional.empty());
		if (options.isRecoveredAndRollback()) {
			// todo: consider doing recover-rollback later (e.g. after the 1st checkpoint)
			// when we are sure that all other subtasks started and committed any of their prepared transactions
			// this would require to distinguish between this job Xids and other Xids
			recoverAndRollback();
		}
		beginTx(0L);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		LOG.debug("snapshot state, checkpointId={}", context.getCheckpointId());
		rollbackPreparedFromCheckpoint(context.getCheckpointId());
		prepareCurrentTx(context.getCheckpointId());
		beginTx(context.getCheckpointId() + 1);
		stateHandler.store(of(preparedXids, hangingXids));
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		commitUpToCheckpoint(Optional.of(checkpointId));
	}

	@Override
	public void invoke(T value, @SuppressWarnings("rawtypes") Context context) throws IOException {
		Preconditions.checkState(currentXid != null, "current xid must not be null");
		if (LOG.isTraceEnabled()) {
			LOG.trace("invoke, xid: {}, value: {}", currentXid, value);
		}
		format.writeRecord(converter.apply(value));
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (currentXid != null && xaFacade.isOpened()) {
			try {
				LOG.debug("remove current transaction before closing, xid={}", currentXid);
				xaFacade.failOrRollback(currentXid);
			} catch (Exception e) {
				LOG.warn("unable to fail/rollback current transaction, xid={}", currentXid, e);
			}
		}
		xaFacade.close();
		xidGenerator.close();
		// don't format.close(); as we don't want neither to flush nor to close connection here
		currentXid = null;
		hangingXids = null;
		preparedXids = null;
	}

	private void prepareCurrentTx(long checkpointId) {
		Preconditions.checkState(currentXid != null, "no current xid");
		Preconditions.checkState(!hangingXids.isEmpty() && hangingXids.peek().equals(currentXid), "inconsistent internal state");
		hangingXids.poll();
		format.flush();
		try {
			xaFacade.endAndPrepare(currentXid);
			preparedXids.add(CheckpointAndXid.createNew(checkpointId, currentXid));
		} catch (EmptyXaTransactionException e) {
			LOG.info("empty XA transaction (skip), xid: {}, checkpoint {}", currentXid, checkpointId);
		}
		currentXid = null;
	}

	/**
	 * @param checkpointId to associate with the new transaction.
	 */
	private void beginTx(long checkpointId) {
		Preconditions.checkState(currentXid == null, "currentXid not null");
		currentXid = xidGenerator.generateXid(getRuntimeContext(), checkpointId);
		hangingXids.offer(currentXid);
		xaFacade.start(currentXid);
	}

	private void commitUpToCheckpoint(Optional<Long> checkpointInclusive) {
		Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> commitAndKeep = partition(preparedXids, checkpointInclusive, true);
		if (commitAndKeep.f0.isEmpty()) {
			checkpointInclusive.ifPresent(cp -> LOG.warn("nothing to commit up to checkpoint: {}", cp));
			return;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("commit {} transactions{}", commitAndKeep.f0.size(), checkpointInclusive.map(i -> " up to checkpoint: " + i).orElse(""));
		}
		CommitResult res = commit(commitAndKeep.f0);
		res.throwIfAnyFailed();
		res.throwIfAnyReachedMaxAttempts(options.getMaxCommitAttempts());
		res.getTransientFailure().ifPresent(f -> LOG.warn("failed to commit {} transactions out of {} (keep them to retry later)", res.getToRetry().size(), commitAndKeep.f0.size(), f));
		preparedXids = commitAndKeep.f1;
		preparedXids.addAll(res.getToRetry());
	}

	private CommitResult commit(List<CheckpointAndXid> xids) {
		CommitResult result = new CommitResult();
		for (Iterator<CheckpointAndXid> i = xids.iterator(); i.hasNext() && (result.noFailures() || options.isAllowOutOfOrderCommits()); ) {
			CheckpointAndXid x = i.next();
			i.remove();
			try {
				xaFacade.commit(x.xid, x.restored);
				result.succeeded(x);
			} catch (TransientXaException e) {
				result.failedTransiently(x, e);
			} catch (Exception e) {
				result.failed(x, e);
			}
		}
		result.getToRetry().addAll(xids);
		return result;
	}

	private void recoverAndRollback() {
		Collection<Xid> recovered = xaFacade.recover();
		if (recovered.isEmpty()) {
			return;
		}
		LOG.warn("rollback {} recovered transactions", recovered.size());
		for (Xid xid : recovered) {
			try {
				xaFacade.rollback(xid);
			} catch (Exception e) {
				LOG.info("unable to rollback recovered transaction, xid={}", xid, e);
			}
		}
	}

	private void rollbackPreparedFromCheckpoint(long fromCheckpointInclusive) {
		Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> keepAndRollback = partition(preparedXids, fromCheckpointInclusive, false);
		if (!keepAndRollback.f1.isEmpty()) {
			LOG.warn("state snapshots have already been taken for checkpoint >= {}, rolling back {} transactions and removing snapshots", fromCheckpointInclusive, keepAndRollback.f1.size());
			failOrRollback(keepAndRollback.f1.stream().map(CheckpointAndXid::getXid).collect(Collectors.toList())) // todo: just rollback without fail?
					.forEach(hangingXids::offerFirst);
		}
		preparedXids = keepAndRollback.f0;
	}

	private List<Xid> failOrRollback(Collection<Xid> xids) {
		if (xids.isEmpty()) {
			return Collections.emptyList();
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("rolling back {} transactions: {}", xids.size(), xids);
		}
		List<Xid> toRetry = new LinkedList<>();
		for (Xid x : xids) {
			try {
				xaFacade.failOrRollback(x);
			} catch (TransientXaException e) {
				LOG.info("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
				toRetry.add(x);
			} catch (Exception e) {
				LOG.warn("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
			}
		}
		if (!toRetry.isEmpty()) {
			LOG.info("failed to roll back {} transactions", toRetry.size());
		}
		return toRetry;
	}

	private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> partition(List<CheckpointAndXid> list, Optional<Long> checkpointInclusive, boolean checkpointIntoLo) {
		return checkpointInclusive
				.map(cp -> partition(preparedXids, cp, checkpointIntoLo))
				.orElse(new Tuple2<>(list, new ArrayList<>()));
	}

	private Tuple2<List<CheckpointAndXid>, List<CheckpointAndXid>> partition(List<CheckpointAndXid> list, long checkpoint, boolean checkpointIntoLo) {
		List<CheckpointAndXid> lo = new ArrayList<>(list.size() / 2);
		List<CheckpointAndXid> hi = new ArrayList<>(list.size() / 2);
		list.forEach(i -> {
			if (i.checkpointId < checkpoint || (i.checkpointId == checkpoint && checkpointIntoLo)) {
				lo.add(i);
			} else {
				hi.add(i);
			}
		});
		return new Tuple2<>(lo, hi);
	}

}
