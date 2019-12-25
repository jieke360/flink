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
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects.JdbcDialectName;
import org.apache.flink.util.Preconditions;

/**
 * JDBC upsert sink options.
 */
@PublicEvolving
public class JdbcUpsertOptions extends JdbcUpdateQueryOptions {

	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_MAX_RETRY_TIMES = 3;

	private final String[] fieldNames;
	private final String[] keyFields;
	private final String tableName;
	private final JdbcDialectName dialect;
	private final int maxRetries;

	public static JdbcUpsertOptionsBuilder builder() {
		return new JdbcUpsertOptionsBuilder();
	}

	private JdbcUpsertOptions(String tableName, JdbcDialectName dialect, String[] fieldNames, int[] fieldTypes, String[] keyFields, int maxRetries) {
		super(fieldTypes);
		this.tableName = Preconditions.checkNotNull(tableName, "table is empty");
		this.dialect = Preconditions.checkNotNull(dialect, "dialect is empty");
		this.fieldNames = Preconditions.checkNotNull(fieldNames, "field names is empty");
		this.keyFields = keyFields;
		this.maxRetries = maxRetries;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public String getTableName() {
		return tableName;
	}

	public JdbcDialectName getDialect() {
		Preconditions.checkNotNull(dialect, "dialect not set");
		return dialect;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public String[] getKeyFields() {
		return keyFields;
	}

	/**
	 * JdbcUpsertOptionsBuilder.
	 */
	public static class JdbcUpsertOptionsBuilder extends JdbcUpdateQueryOptionsBuilder<JdbcUpsertOptionsBuilder> {
		private String tableName;
		private String[] fieldNames;
		private String[] keyFields;
		private JdbcDialectName dialect;
		private int maxRetries = DEFAULT_MAX_RETRY_TIMES;

		public JdbcUpsertOptionsBuilder withMaxRetries(int maxRetries) {
			this.maxRetries = maxRetries;
			return this;
		}

		@Override
		protected JdbcUpsertOptionsBuilder self() {
			return this;
		}

		public JdbcUpsertOptionsBuilder withFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		public JdbcUpsertOptionsBuilder withKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		public JdbcUpsertOptionsBuilder withTableName(String tableName) {
			this.tableName = tableName;
			return self();
		}

		public JdbcUpsertOptionsBuilder withDialect(JdbcDialectName dialect) {
			this.dialect = dialect;
			return self();
		}

		public JdbcUpsertOptions build() {
			return new JdbcUpsertOptions(tableName, dialect, fieldNames, fieldTypes, keyFields, maxRetries);
		}
	}
}
