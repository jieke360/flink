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

package org.apache.flink.api.java.io.jdbc.xa.h2;

import org.h2.api.ErrorCode;
import org.h2.jdbcx.JdbcXAConnection;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps H2 {@link XAResource} to:
 * <ol>
 *     <li>reset <code>currentTransaction</code> field after a call to {@link XAResource#prepare prepare}.
 *     This allows to {@link XAResource#start start} a new transaction after preparing the current one.
 *     (see this <a href="http://h2-database.66688.n3.nabble.com/Possible-XA-bug-and-fix-td3303095.html">discussion</a>)</li>
 *     <li>prevent {@link NullPointerException} when there is no active XA transaction in {@link XAResource#start start}, {@link XAResource#end end} (and throw appropriate {@link XAException} instead)</li>
 *     <li>prevent {@link XAResource#commit commit} or {@link XAResource#rollback rollback} when there is no active XA transaction</li>
 * </ol>.
 * <p> TODO: fix the issue in the upstream. </p>
 */
public class H2XaResourceWrapper implements XAResource {

	private static final Field CURRENT_TRANSACTION_FIELD;
	private static final Field IS_PREPARED_FIELD;

	static {
		try {
			CURRENT_TRANSACTION_FIELD = JdbcXAConnection.class.getDeclaredField("currentTransaction");
			CURRENT_TRANSACTION_FIELD.setAccessible(true);
			IS_PREPARED_FIELD = JdbcXAConnection.class.getDeclaredField("prepared");
			IS_PREPARED_FIELD.setAccessible(true);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		}
	}

	private final XAResource wrapped;
	private final Set<Xid> committed = new HashSet<>();

	H2XaResourceWrapper(XAResource wrapped) {
		this.wrapped = wrapped;
	}

	@Override
	public void commit(Xid xid, boolean onePhase) throws XAException {
		Object currentTransaction = getCurrentTransaction();
		if (onePhase) {
			ensureInTx();
			if (!currentTransaction.equals(xid)) {
				throw new XAException(XAException.XAER_OUTSIDE);
			}
		}
		try {
			wrapped.commit(xid, onePhase);
		} catch (XAException e) {
			if (e.getCause() instanceof SQLException) {
				if (((SQLException) e.getCause()).getErrorCode() == ErrorCode.TRANSACTION_NOT_FOUND_1) {
					throw new XAException(XAException.XAER_NOTA);
				}
			}
			throw e;
		}
		committed.add(xid);
		if (!currentTransaction.equals(xid)) { // restore tx
			setCurrentTransaction(currentTransaction);
		}
	}

	@Override
	public void rollback(Xid xid) throws XAException {
		if (!isPrepared()) {
			ensureInTx();
		}
		wrapped.rollback(xid);
	}

	@Override
	public void end(Xid xid, int i) throws XAException {
		if (committed.contains(xid)) {
			throw new XAException(XAException.XA_RDONLY);
		}
		ensureInTx();
		wrapped.end(xid, i);
	}

	@Override
	public void forget(Xid xid) throws XAException {
		wrapped.forget(xid);
		resetCurrentTx(wrapped);
	}

	@Override
	public int getTransactionTimeout() throws XAException {
		return wrapped.getTransactionTimeout();
	}

	@Override
	public boolean isSameRM(XAResource xaResource) throws XAException {
		return wrapped.isSameRM(xaResource);
	}

	@Override
	public int prepare(Xid xid) throws XAException {
		ensureInTx();
		int ret = wrapped.prepare(xid);
		resetCurrentTx(wrapped);
		return ret;
	}

	@Override
	public Xid[] recover(int i) throws XAException {
		return wrapped.recover(i);
	}

	@Override
	public boolean setTransactionTimeout(int i) throws XAException {
		return wrapped.setTransactionTimeout(i);
	}

	@Override
	public void start(Xid xid, int i) throws XAException {
		wrapped.start(xid, i);
	}

	private boolean isPrepared() {
		try {
			return (boolean) IS_PREPARED_FIELD.get(wrapped);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	private static void resetCurrentTx(XAResource wr) {
		try {
			CURRENT_TRANSACTION_FIELD.set(wr, null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Object getCurrentTransaction() {
		try {
			return CURRENT_TRANSACTION_FIELD.get(wrapped);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	private void ensureInTx() throws XAException {
		if (getCurrentTransaction() == null) {
			throw new XAException(XAException.XAER_OUTSIDE);
		}
	}

	private void setCurrentTransaction(Object currentTransaction) {
		try {
			CURRENT_TRANSACTION_FIELD.set(wrapped, currentTransaction);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

}
