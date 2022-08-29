package cs455.aqi;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q4_Reducer extends Reducer<Text, LongWritable, LongWritable, Text> {
	// <aqi, county>
	private TreeMap<Long, String> tmap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		tmap = new TreeMap<Long, String>();
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// county : aqi
		String county = key.toString();
		long sum = 0;
		long nValues = 0;

		for (LongWritable val : values) {
			nValues += 1;
			sum += val.get();
		}

		if (nValues != 0) {
			long avg = sum / nValues;
			tmap.put(avg, county);
		}

		tmap = checkWorst10(tmap);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Long, String> entry : tmap.entrySet()) {
			long aqiAvg = entry.getKey();
			String county = entry.getValue();
			// We only write at the end so we ensure we got the Worst 10.
			context.write(new LongWritable(aqiAvg), new Text(county));
		}
	}

	// Keep size 10 unless there is a tie in aqi to get worst 10.
	private static TreeMap<Long, String> checkWorst10(TreeMap<Long, String> tm) {
		if (tm.size() <= 10)
			return tm;
		else {
			Iterator<Entry<Long, String>> worst10Iter = tm.entrySet().iterator();
			Entry<Long, String> worst10Entry;
			Long prevAqi = 0L;
			int counter = 0;

			while (worst10Iter.hasNext()) {
				counter++;
				worst10Entry = worst10Iter.next();

				
				if (counter > 10) {
					if (worst10Entry.getKey() > prevAqi) {
						worst10Iter.remove();
						continue;
					}
				}
				prevAqi = worst10Entry.getKey();

			}

		}
		return tm;
	}
}
