package dataflow;

import com.google.api.client.repackaged.com.google.common.base.Objects;
import com.google.api.client.util.Preconditions;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * An iterator over an infinite stream of {@link GameActionInfo} objects.
 */
public class InjectorIterator implements Iterator<PackageActivityInfo> {

	/** The parameters for the Injector. */
	public static class SourceConfig implements Serializable {
		public final int minQps;
		public final int maxQps;
		public final int minTruck;
		public final int maxTruck;

		public SourceConfig(int minQps, int maxQps) {
			this.minQps = minQps; // Minimum truck incluse
			this.maxQps = maxQps; // Max truck exclusie.
			this.minTruck = 0;
			this.maxTruck = TRUCKS.size() - 1;
		}
		
		protected SourceConfig(int minQps, int maxQps, int minTruck, int maxTruck) {
			this.minQps = minQps; // Minimum truck incluse
			this.maxQps = maxQps; // Max truck exclusie.
			this.minTruck = minTruck;
			this.maxTruck = maxTruck;
		}

		/**
		 * Divide a SourceConfig into some number of parts, with the following
		 * constraints:
		 *
		 * 1. Each part should be responsible for an integer number of teams. 2.
		 * Entries and QPS should be divided between splits proportionally to
		 * the teams.
		 */
		public SourceConfig[] split(int numParts) {
			SourceConfig[] parts = new SourceConfig[numParts];
			int totalRange = maxTruck - minTruck;
			int newMinTruck = minTruck;
			int newMaxTruck;
			for (int i = 0; i < numParts; i++) {
				int newRange = totalRange / numParts + (i < totalRange % numParts ? 1 : 0);
				newMaxTruck = newMinTruck + newRange;
				parts[i] = new SourceConfig(newRange * (minQps / totalRange),
						newRange * (maxQps / totalRange), newMinTruck, newMaxTruck);
				newMinTruck = newMaxTruck;
			}
			return parts;
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(InjectorIterator.class);

	// Lists used to generate random team names.
	private static final ArrayList<String> LOCATIONS = new ArrayList<String>(Arrays.asList("AR",
			"ZA", "PA", "QR", "AF", "TE", "AG", "AB", "FR", "Ruby", "MA", "BG", "AA", "AB"));
	private static final ArrayList<Integer> TRUCKS = new ArrayList<Integer>(
			Arrays.asList(2, 43, 13, 134, 533, 566, 100, 424, 234, 122, 100, 32, 453, 134, 455, 345, 234, 122, 44));
	public List<Integer> myTrucks;

	// The minimum time a 'team' can live will be in the range
	// [BASE_TEAM_EXPIRATION, BASE_TEAM_EXPIRATION + RANDOM_TEAM_EXPIRATION)
	private static final Duration BASE_PACKAGE_DURATION = Duration.standardMinutes(20);

	private Random random = new Random();

	// QPS ranges from 800 to 1000
	private final Iterator<Instant> timestamps;

	public InjectorIterator(SourceConfig config) {
		Instant now = Instant.now();
		timestamps = new QpsIterator(random, now, config.minQps, config.maxQps);
		myTrucks = TRUCKS.subList(config.minTruck, config.maxTruck);
	}

	@Override
	public boolean hasNext() {
		return true; // Unbounded
	}

	@Override
	public PackageActivityInfo next() {
		PackageActivityInfo info = new PackageActivityInfo();
		info.location = randomItem(LOCATIONS);
		info.time = timestamps.next().getMillis();
		info.truckId = randomItem(myTrucks);
		Integer i = new Integer(random.nextInt());
		info.packageId = i.toString();
		return info;
	}

	/** Return a random item from the list of items. */
	private <T> T randomItem(List<T> items) {
		return items.get(random.nextInt(items.size()));
	}

	private static class QpsIterator implements Iterator<Instant> {
		private final Random random;
		private final long minDurationNanos;
		private final int durationRangeNanos;

		private Instant lastTimestamp;

		public QpsIterator(Random random, Instant initialTimestamp, int minQps, int maxQps) {
			Preconditions.checkArgument(minQps <= maxQps, "minQps(%1) should be <= maxQps(%2)",
					minQps, maxQps);
			this.random = random;
			lastTimestamp = initialTimestamp;
			if (maxQps == 0) maxQps = 1;
			if (minQps == 0) minQps = 1;
			LOG.info("Max MIN" + maxQps +  " " + minQps);
			
			minDurationNanos = TimeUnit.SECONDS.toNanos(1) / maxQps;
			long maxDurationNanos = TimeUnit.SECONDS.toNanos(1) / minQps;
			durationRangeNanos = Math.max(1, (int) (maxDurationNanos - minDurationNanos));
			Preconditions.checkArgument(durationRangeNanos > 0,
					"durationRangeNanos(%1) should be > 0", durationRangeNanos);
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Instant next() {
			long nextNanos = minDurationNanos + (random.nextInt(durationRangeNanos));
			lastTimestamp = lastTimestamp.plus(TimeUnit.NANOSECONDS.toMillis(nextNanos));
			return lastTimestamp;
		}
	}
}
