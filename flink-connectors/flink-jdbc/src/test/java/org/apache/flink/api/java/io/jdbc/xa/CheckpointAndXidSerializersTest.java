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

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * XaSerializersTest.
 */
public class CheckpointAndXidSerializersTest {
	private static final CheckpointAndXid CHECKPOINT_AND_XID = CheckpointAndXid.createNew(1L, XidImplTest.XID);

	@Test
	public void testXidSerializer() throws IOException {
		CheckpointAndXid d = deserialize(serialize(CHECKPOINT_AND_XID));
		assertEquals(CHECKPOINT_AND_XID.checkpointId, d.checkpointId);
		assertEquals(CHECKPOINT_AND_XID.xid, d.xid);
		assertFalse(CHECKPOINT_AND_XID.restored);
		assertTrue(d.restored);
	}

	private CheckpointAndXid deserialize(byte[] bytes) throws IOException {
		try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
			return new CheckpointAndXidSerializer().deserialize(new DataInputViewStreamWrapper(in));
		}
	}

	private byte[] serialize(CheckpointAndXid xid) throws IOException {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			new CheckpointAndXidSerializer().serialize(xid, new DataOutputViewStreamWrapper(out));
			return out.toByteArray();
		}
	}
}
