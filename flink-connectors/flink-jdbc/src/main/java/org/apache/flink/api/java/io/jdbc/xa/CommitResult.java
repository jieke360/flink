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

import org.apache.flink.api.java.io.jdbc.xa.XaFacade.TransientXaException;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class CommitResult {
	private final List<CheckpointAndXid> committed = new ArrayList<>();
	private final List<CheckpointAndXid> failed = new ArrayList<>();
	private final List<CheckpointAndXid> toRetry = new ArrayList<>();
	private Optional<Exception> failure = Optional.empty();
	private Optional<Exception> transientFailure = Optional.empty();

	void failedTransiently(CheckpointAndXid x, TransientXaException e) {
		toRetry.add(x.withAttemptsIncremented());
		transientFailure = getTransientFailure().isPresent() ? getTransientFailure() : Optional.of(e);
	}

	void failed(CheckpointAndXid x, Exception e) {
		failed.add(x);
		failure = failure.isPresent() ? failure : Optional.of(e);
	}

	void succeeded(CheckpointAndXid x) {
		committed.add(x);
	}

	void throwIfAnyFailed() {
		failure
				.map(f -> wrapFailure(f, "failed to commit %d transactions out of %d", toRetry.size() + failed.size()))
				.ifPresent(f -> {
					throw f;
				});
	}

	void throwIfAnyTransientlyFailed() {
		getTransientFailure()
				.map(f -> wrapFailure(f, "too many transient failures while committing %d prepared transactions: %d", toRetry.size()))
				.ifPresent(f -> {
					throw f;
				});
	}

	private FlinkRuntimeException wrapFailure(Exception error, String formatWithCounts, int errCount) {
		return new FlinkRuntimeException(String.format(formatWithCounts, errCount, total()), error);
	}

	private int total() {
		return committed.size() + failed.size() + toRetry.size();
	}

	List<CheckpointAndXid> getToRetry() {
		return toRetry;
	}

	Optional<Exception> getTransientFailure() {
		return transientFailure;
	}

	void throwIfAnyReachedMaxAttempts(int maxAttempts) {
		List<CheckpointAndXid> reached = null;
		for (CheckpointAndXid x : toRetry) {
			if (x.attempts >= maxAttempts) {
				if (reached == null) {
					reached = new ArrayList<>();
				}
				reached.add(x);
			}
		}
		if (reached != null) {
			throw new RuntimeException(String.format("reached max number of commit attempts (%d) for transactions: %s", maxAttempts, reached));
		}
	}

	boolean noFailures() {
		return !failure.isPresent() && !transientFailure.isPresent();
	}
}
