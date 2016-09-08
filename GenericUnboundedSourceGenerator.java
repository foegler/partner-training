/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package PartnerTraining;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import javax.annotation.Nullable;

/**
 * An {@link UnboundedSource} which generates {@link PackageActivityInfo}
 * events.
 */

public class GenericUnboundedSourceGenerator
		extends PTransform<PBegin, PCollection<PackageActivityInfo>> {
	@Override
	public PCollection<PackageActivityInfo> apply(PBegin input) {
		return input.apply(Read.from(new GenericUnboundedSource()));
	}

	/**
	 * Checkpointing an {@link GenericUnboundedSource} requires remembering our
	 * random number generator, among other things.
	 */
	public static class Checkpoint implements UnboundedSource.CheckpointMark, Serializable {

		private final Random random;

		public Checkpoint(Random random) {
			this.random = SerializableUtils.clone(random);
		}

		@Override
		public void finalizeCheckpoint() throws IOException {
			// Nothing is necessary for checkpoints.
		}

		public Random getRandom() {
			return random;
		}
	}

	private static class InjectorUnboundedReader extends UnboundedReader<PackageActivityInfo> {

		private final GenericUnboundedSource source;
		private final Random random;
		private final InjectorIterator items;

		private PackageActivityInfo currentEvent = null;
		private PackageActivityInfo nextEvent;
		private Instant nextEventTimestamp;

		// The "watermark" is simulated to be this far behind the current
		// processing time. All arriving
		// data is generated within this delay from now, with a high
		// probability (80%)
		private static final Duration WATERMARK_DELAY = Duration.standardSeconds(5);

		// 15% of the data will be between WATERMARK_DELAY and
		// WATERMARK_DELAY + LATE_DELAY of now.
		// 5% of the data will be between WATERMARK_DELAY + LATE_DELAY and
		// WATERMARK_DELAY + 2 * LATE_DELAY of now.
		private static final Duration LATE_DELAY = Duration.standardSeconds(25);

		private static final int PERCENT_ONE_UNITS_LATE = 7;
		private static final int PERCENT_TWO_UNITS_LATE = 3;

		public InjectorUnboundedReader(GenericUnboundedSource source,
				@Nullable Checkpoint initialCheckpoint) {
			this.source = source;
			this.items = new InjectorIterator(source.config);
			random = initialCheckpoint == null ? new Random() : initialCheckpoint.getRandom();

			// TODO: Should probably put nextArrivalTime and currentEvent
			// into the checkpoint?
			nextEvent = items.next();
			nextEventTimestamp = new Instant(nextEvent.getTime());
		}

		@Override
		public boolean start() throws IOException {
			return advance();
		}

		private Instant arrivalTimeToEventTime(Instant arrivalTime) {
			int bucket = random.nextInt(100);
			Instant eventTime = arrivalTime;
			if (bucket <= PERCENT_ONE_UNITS_LATE) {
				eventTime = eventTime.minus(LATE_DELAY);
			}
			if (bucket <= PERCENT_ONE_UNITS_LATE + PERCENT_TWO_UNITS_LATE) {
				eventTime = eventTime.minus(WATERMARK_DELAY);
				eventTime = eventTime.minus(randomDuration(LATE_DELAY));
			} else {
				eventTime = eventTime.minus(randomDuration(WATERMARK_DELAY));
			}
			return eventTime;
		}

		private Duration randomDuration(Duration max) {
			return Duration.millis(random.nextInt((int) max.getMillis()));
		}

		@Override
		public boolean advance() throws IOException {
			if (nextEventTimestamp.isAfterNow()) {
				// Not yet ready to emit the next event
				currentEvent = null;
				return false;
			} else {
				// The next event is available now. Figure out what its
				// actual
				// event time was:
				currentEvent = new PackageActivityInfo(nextEvent.isArrival(),
						nextEvent.getLocation(), nextEvent.getTime(), nextEvent.getTruckId(),
						nextEvent.getPackageId());

				// And we should peek to see when the next event will be
				// ready.
				nextEvent = items.next();
				nextEventTimestamp = new Instant(currentEvent.getTime());
				return true;
			}
		}

		@Override
		public Instant getWatermark() {
			return Instant.now().minus(WATERMARK_DELAY);
		}

		@Override
		public Checkpoint getCheckpointMark() {
			return new Checkpoint(random);
		}

		@Override
		public UnboundedSource<PackageActivityInfo, ?> getCurrentSource() {
			return source;
		}

		@Override
		public PackageActivityInfo getCurrent() throws NoSuchElementException {
			if (currentEvent == null) {
				throw new NoSuchElementException("No current element");
			}
			return currentEvent;
		}

		@Override
		public Instant getCurrentTimestamp() throws NoSuchElementException {
			return new Instant(getCurrent().getTime());
		}

		@Override
		public void close() throws IOException {
			// Nothing is necessary to close.
		}
	}

	private class GenericUnboundedSource extends
			UnboundedSource<PackageActivityInfo, GenericUnboundedSourceGenerator.Checkpoint> {

		private final InjectorIterator.SourceConfig config;

		public GenericUnboundedSource() {
			this(new InjectorIterator.SourceConfig(5000, 8000));
		}

		private GenericUnboundedSource(InjectorIterator.SourceConfig config) {
			this.config = config;
		}

		@Override
		public List<GenericUnboundedSource> generateInitialSplits(int desiredNumSplits,
				PipelineOptions options) throws Exception {
			int numSplits = Math.min(desiredNumSplits, config.maxTruck - config.minTruck);
			ArrayList<GenericUnboundedSource> splits = new ArrayList<>(numSplits);
			for (InjectorIterator.SourceConfig config : config.split(numSplits)) {
				splits.add(new GenericUnboundedSource(config));
			}
			return splits;
		}

		@Override
		public InjectorUnboundedReader createReader(PipelineOptions options,
				@Nullable Checkpoint checkpoint) {
			return new InjectorUnboundedReader(this, checkpoint);
		}

		@Override
		public Coder<Checkpoint> getCheckpointMarkCoder() {
			return SerializableCoder.of(Checkpoint.class);
		}

		@Override
		public void validate() {
			// Nothing to validate
		}

		@Override
		public Coder<PackageActivityInfo> getDefaultOutputCoder() {
			return AvroCoder.of(PackageActivityInfo.class);
		}
	}
}
