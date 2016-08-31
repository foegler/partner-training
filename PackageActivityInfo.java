package PartnerTraining;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;

/* Simple class representing metadata about a package's pickup or
 * dropoff. Is meant to be read from a log line. 
 */
@DefaultCoder(AvroCoder.class)
public class PackageActivityInfo {
	private static final Logger LOG = LoggerFactory.getLogger(PackageActivityInfo.class);
	
	private boolean isArrival; // If true, is an arrival event; otherwise departing.
	private String location; // Two letter code for factory id or "CU" for customer counter.
	private Date time; // Time of the drop off or pickup activity.
	private int truckId; // ID of the truck doing the drop off or pick up. 0 if customer.
	private String packageId; // ID of the package in transit.

	@Override
	public String toString() {
		return "PackageActivityInfo [isArrival=" + isArrival + ", location=" + location + ", packageId=" + packageId + "]";
	}

	public boolean isArrival() {
		return isArrival;
	}

	public String getLocation() {
		return location;
	}

	public Date getTime() {
		return time;
	}

	public int getTruckId() {
		return truckId;
	}

	public String getPackageId() {
		return packageId;
	}
	
	// Return null if there is any error parsing.
	// Delimits line by the given delimeter.
	// Logline: "0, AN, 1467394122, 423, 372A3SZ4J98"
	public static PackageActivityInfo Parse(String logLine) {
		try {
			PackageActivityInfo pickup = new PackageActivityInfo();
			String[] pieces = logLine.split(",");
			if (pieces.length != 5)
				return null;
			int isArrivalInt = Integer.parseInt(pieces[0].trim());
			if (isArrivalInt == 0) {
				pickup.isArrival = false;
			} else if (isArrivalInt == 1) {
				pickup.isArrival = true;
			} else {
				return null;
			}
			pickup.location = pieces[1].trim();
			pickup.time = new Date(Long.parseLong(pieces[2].trim()));
			pickup.truckId = Integer.parseInt(pieces[3].trim());
			pickup.packageId = pieces[4].trim();
			return pickup;

		} catch (Exception e) {
			return null; // Return null if any error parsing.
		}
	}
	
	// Parses the package log as if it was an older file (before
	// multiple locations). In this case all logs were departures
	// and the location was always from the one location "AR".
	public static PackageActivityInfo ParseOld(String logline) {
		try {
			PackageActivityInfo pickup = new PackageActivityInfo();
			String[] pieces = logline.split(",");
			if (pieces.length != 3)
				return null;
			pickup.isArrival = false;
			pickup.location = "AR";
			pickup.time = new Date(Long.parseLong(pieces[2].trim()));
			pickup.truckId = Integer.parseInt(pieces[3].trim());
			pickup.packageId = pieces[4].trim();
			return pickup;

		} catch (Exception e) {
			return null; // Return null if any error parsing.
		}
	}
	
	static class ParseLine extends DoFn<String, PackageActivityInfo> {
		private final Aggregator<Long, Long> invalidLines = createAggregator("invalidLogLines", new Sum.SumLongFn());

		@Override
		public void processElement(ProcessContext c) {
			String logLine = c.element();
			PackageActivityInfo info = PackageActivityInfo.Parse(logLine);
			if (info == null) {
				invalidLines.addValue(1L);
			} else {
				c.output(info);
			}
		}
	}
	
	public static final String[] MINI_LOG = {"0, AN, 1467394122, 423, 372A3SZ4J98",
				          "0, AN, 1467394122, 423##############",
				          "404 - broken message",
				          "0, AN, 1467394122, 423, 372A3SZ4J98"};
}