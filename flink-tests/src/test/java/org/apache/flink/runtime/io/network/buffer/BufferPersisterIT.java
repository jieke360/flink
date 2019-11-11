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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests that a buffer persister indeed spills the required data onto disk.
 */
public class BufferPersisterIT extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final Timeout timeout = Timeout.builder()
		.withTimeout(30, TimeUnit.SECONDS)
		.withLookingForStuckThread(true)
		.build();

	@Test
	public void testSimplePersist() throws Exception {
		final File persistDir = temporaryFolder.newFolder();
		StreamExecutionEnvironment env = createEnv(persistDir, 1);

		// waits until we get 10 output files in the persist folder. That should take ~1s.
		final TestListResultSink<Integer> sink = createDAG(env, persistDir, 10);
		env.execute();

		// each record has a header and a payload, that is 9 bytes.
		// with two buffer persisters that's a total of 18 bytes.
		// watermarks can add additional bytes, so we are lenient in the check
		long expectedSize = sink.getResult().size() * 18;
		assertTrue(getTotalSize(persistDir) > expectedSize);
	}

	@Test
	public void testParallelPersist() throws Exception {
		final File persistDir = temporaryFolder.newFolder();
		StreamExecutionEnvironment env = createEnv(persistDir, 2);

		// waits until we get 20 output files in the persist folder. That should take ~1s.
		final TestListResultSink<Integer> sink = createDAG(env, persistDir, 20);
		env.execute();

		// each record has a header and a payload, that is 9 bytes.
		// with two buffer persisters that's a total of 18 bytes.
		// watermarks can add additional bytes, so we are lenient in the check
		long expectedSize = sink.getResult().size() * 18;
		assertTrue(getTotalSize(persistDir) > expectedSize);
	}

	@Nonnull
	private static LocalStreamEnvironment createEnv(final File persistDir, final int parallelism) {
		Configuration conf = new Configuration();
		conf.setString(TaskManagerOptions.LEGACY_MANAGED_MEMORY_SIZE, "4m");
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, parallelism);
		conf.setString(CheckpointingOptions.PERSIST_LOCATION_CONFIG, persistDir.toURI().toString());
		final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
		env.getCheckpointConfig().setCheckpointInterval(100);
		return env;
	}

	private long getTotalSize(final File persistDir) throws IOException {
		return Files.walk(persistDir.toPath()).mapToLong(p -> {
			try {
				return Files.size(p);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}).sum();
	}

	private TestListResultSink<Integer> createDAG(
			final StreamExecutionEnvironment env,
			final File persistDir,
			final int maxParts) {
		final SingleOutputStreamOperator<Integer> source = env.addSource(new IntegerSource(persistDir, maxParts));
		final SingleOutputStreamOperator<Integer> transform = source.shuffle().map(i -> 2 * i);
		final TestListResultSink<Integer> sink = new TestListResultSink<>();
		transform.shuffle().addSink(sink);
		return sink;
	}

	private static class IntegerSource implements ParallelSourceFunction<Integer> {
		private final File persistDir;
		private final int maxParts;

		private volatile boolean running = true;

		public IntegerSource(final File persistDir, final int maxParts) {
			this.persistDir = persistDir;
			this.maxParts = Integer.MAX_VALUE;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			int counter = 0;
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(counter++);
				}

				if (counter % 1000 == 0 && getPartCount(persistDir) > maxParts) {
					cancel();
				}
			}
		}

		private long getPartCount(final File persistDir) {
			if(persistDir.isDirectory()) {
				return Arrays.stream(persistDir.listFiles()).mapToLong(this::getPartCount).sum();
			}
			return !persistDir.getName().startsWith(".") ? 1 : 0;
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
