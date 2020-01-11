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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A simple {@link Xid} implementation that stores branch and global transaction identifiers as byte arrays.
 */
@Internal
final class XidImpl implements Xid, Serializable {

	private final int formatId;
	@Nonnull
	private final byte[] globalTransactionId;
	@Nonnull
	private final byte[] branchQualifier;

	XidImpl(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
		Preconditions.checkArgument(globalTransactionId.length <= Xid.MAXGTRIDSIZE);
		Preconditions.checkArgument(branchQualifier.length <= Xid.MAXBQUALSIZE);
		this.formatId = formatId;
		this.globalTransactionId = Arrays.copyOf(globalTransactionId, globalTransactionId.length);
		this.branchQualifier = Arrays.copyOf(branchQualifier, branchQualifier.length);
	}

	@Override
	public int getFormatId() {
		return formatId;
	}

	@Override
	public byte[] getGlobalTransactionId() {
		return globalTransactionId;
	}

	@Override
	public byte[] getBranchQualifier() {
		return branchQualifier;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof XidImpl)) {
			return false;
		}
		XidImpl xid = (XidImpl) o;
		return formatId == xid.formatId &&
				Arrays.equals(globalTransactionId, xid.globalTransactionId) &&
				Arrays.equals(branchQualifier, xid.branchQualifier);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(formatId);
		result = 31 * result + Arrays.hashCode(globalTransactionId);
		result = 31 * result + Arrays.hashCode(branchQualifier);
		return result;
	}

	@Override
	public String toString() {
		return formatId + ":" + bytesToHex(globalTransactionId) + ":" + bytesToHex(branchQualifier);
	}

	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	private static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars);
	}}
