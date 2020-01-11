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

import org.junit.Test;

import java.util.Random;

import static javax.transaction.xa.Xid.MAXBQUALSIZE;
import static javax.transaction.xa.Xid.MAXGTRIDSIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for {@link XidImpl}.
 */
public class XidImplTest {
	static final XidImpl XID = new XidImpl(1, randomBytes(MAXGTRIDSIZE), randomBytes(MAXBQUALSIZE));

	@Test
	public void testXidsEqual() {
		XidImpl other = new XidImpl(XID.getFormatId(), XID.getGlobalTransactionId(), XID.getBranchQualifier());
		assertEquals(XID, other);
		assertEquals(XID.hashCode(), other.hashCode());
	}

	@Test
	public void testXidsNotEqual() {
		assertNotEquals(XID, new XidImpl(0, XID.getGlobalTransactionId(), XID.getBranchQualifier()));
		assertNotEquals(XID, new XidImpl(XID.getFormatId(), randomBytes(MAXGTRIDSIZE), XID.getBranchQualifier()));
		assertNotEquals(XID, new XidImpl(XID.getFormatId(), XID.getGlobalTransactionId(), randomBytes(MAXBQUALSIZE)));
	}

	private static byte[] randomBytes(int size) {
		return fillRandom(new byte[size]);
	}

	private static byte[] fillRandom(byte[] bytes) {
		new Random().nextBytes(bytes);
		return bytes;
	}
}
