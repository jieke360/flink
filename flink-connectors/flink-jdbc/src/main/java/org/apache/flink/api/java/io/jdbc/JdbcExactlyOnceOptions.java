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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Jdbc exactly once sink options.
 *
 * <p><b>maxCommitAttempts</b> -
 * maximum number of commit attempts to make per transaction; must be > 0; state size is proportional to the product of max number of in-flight snapshots and this number.
 *
 * <p><b>allowOutOfOrderCommits</b> -
 * If true, all prepared transactions will be attempted to commit regardless of any transient failures during this operation.
 * This may lead to inconsistency.
 * Default: false.
 *
 * <p><b>recoveredAndRollback</b> -
 * whether to rollback prepared transactions known to XA RM on startup (after committing <b>known</b> transactions, i.e. restored from state).
 *
 * <p>NOTE that setting this parameter to true may:<ol>
 * <li>interfere with other subtasks or applications (one subtask rolling back transactions prepared by the other one (and known to it))</li>
 * <li>block when using with some non-MVCC databases, if there are ended-not-prepared transactions</li>
 * </ol>
 * See also {@link org.apache.flink.api.java.io.jdbc.xa.XaFacade#recover()}
 * </p>
 */
public class JdbcExactlyOnceOptions implements Serializable {

	private static final boolean DEFAULT_RECOVERED_AND_ROLLBACK = false;
	private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 3;
	public static final boolean DEFAULT_ALLOW_OUT_OF_ORDER_COMMITS = false;

	private final boolean recoveredAndRollback;
	private final int maxCommitAttempts;
	private final boolean allowOutOfOrderCommits;

	private JdbcExactlyOnceOptions(boolean recoveredAndRollback, int maxCommitAttempts, boolean allowOutOfOrderCommits) {
		this.recoveredAndRollback = recoveredAndRollback;
		this.maxCommitAttempts = maxCommitAttempts;
		this.allowOutOfOrderCommits = allowOutOfOrderCommits;
		Preconditions.checkArgument(this.maxCommitAttempts > 0, "maxCommitAttempts should be > 0");
	}

	public static JdbcExactlyOnceOptions defaults() {
		return builder().build();
	}

	public boolean isRecoveredAndRollback() {
		return recoveredAndRollback;
	}

	public boolean isAllowOutOfOrderCommits() {
		return allowOutOfOrderCommits;
	}

	public int getMaxCommitAttempts() {
		return maxCommitAttempts;
	}

	public static JdbcExactlyOnceOptionsBuilder builder() {
		return new JdbcExactlyOnceOptionsBuilder();
	}

	/**
	 * JdbcExactlyOnceOptionsBuilder.
	 */
	public static class JdbcExactlyOnceOptionsBuilder {
		private boolean recoveredAndRollback = DEFAULT_RECOVERED_AND_ROLLBACK;
		private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
		private boolean allowOutOfOrderCommits = DEFAULT_ALLOW_OUT_OF_ORDER_COMMITS;

		public JdbcExactlyOnceOptionsBuilder withRecoveredAndRollback(boolean recoveredAndRollback) {
			this.recoveredAndRollback = recoveredAndRollback;
			return this;
		}

		public JdbcExactlyOnceOptionsBuilder withMaxCommitAttempts(int maxCommitAttempts) {
			this.maxCommitAttempts = maxCommitAttempts;
			return this;
		}

		public JdbcExactlyOnceOptionsBuilder withAllowOutOfOrderCommits(boolean allowOutOfOrderCommits) {
			this.allowOutOfOrderCommits = allowOutOfOrderCommits;
			return this;
		}

		public JdbcExactlyOnceOptions build() {
			return new JdbcExactlyOnceOptions(recoveredAndRollback, maxCommitAttempts, allowOutOfOrderCommits);
		}
	}
}
