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

import javax.transaction.xa.Xid;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * XaSerializersTest.
 */
public class XidSerializersTest {

	@Test
	public void testXidSerializer() throws IOException {
		assertEquals(XidImplTest.XID, deserialize(serialize(XidImplTest.XID)));
	}

	private Xid deserialize(byte[] bytes) throws IOException {
		try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
			return new XidSerializer().deserialize(new DataInputViewStreamWrapper(in));
		}
	}

	private byte[] serialize(Xid xid) throws IOException {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			new XidSerializer().serialize(xid, new DataOutputViewStreamWrapper(out));
			return out.toByteArray();
		}
	}
}
