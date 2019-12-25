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
import org.apache.flink.util.Preconditions;

/**
 * JDBC sink insert options.
 */
@PublicEvolving
public class JdbcInsertOptions extends JdbcUpdateQueryOptions {

	private static final long serialVersionUID = 1L;

	private final String query;

	private JdbcInsertOptions(String query, int[] typesArray) {
		super(typesArray);
		this.query = Preconditions.checkNotNull(query, "query is empty");
	}

	public String getQuery() {
		return query;
	}

	public static JdbcInsertOptionsBuilder builder() {
		return new JdbcInsertOptionsBuilder();
	}

	/**
	 * JdbcInsertOptionsBuilder.
	 */
	public static class JdbcInsertOptionsBuilder extends JdbcUpdateQueryOptionsBuilder<JdbcInsertOptionsBuilder> {
		private String query;

		@Override
		protected JdbcInsertOptionsBuilder self() {
			return this;
		}

		public JdbcInsertOptionsBuilder withQuery(String query) {
			this.query = query;
			return this;
		}

		public JdbcInsertOptions build() {
			return new JdbcInsertOptions(query, fieldTypes);
		}
	}
}
