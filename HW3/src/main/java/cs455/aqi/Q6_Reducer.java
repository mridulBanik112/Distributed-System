package cs455.aqi;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q6_Reducer extends Reducer<Text, Text, IntWritable, Text> {
	private HashMap<String, Long> aqiStateRankHmap; // <state, aqiRank>
	private HashMap<String, Integer> aqiStateNRefineriesHmap; // <state, nRefineriesRank>

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		aqiStateRankHmap = new HashMap<String, Long>();
		aqiStateNRefineriesHmap = new HashMap<String, Integer>();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String state = key.toString();

		for (Text val : values) {
			String[] tokens = val.toString().split(";");
			String prefix = tokens[0];
			if (prefix.equals("aqi")) {
				Long aqiRank = Long.parseLong(tokens[1]);
				aqiStateRankHmap.put(state, aqiRank);
			} else if (prefix.equals("refineries")) {
				Integer nRefineRank = Integer.parseInt(tokens[1]);
				aqiStateNRefineriesHmap.put(state, nRefineRank);
			}
		}
	}

	/*
	 * stateRankTmap = AQI Rank + n Refineries Rank. write highest combined rank to
	 * lowest
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		// Show highest stateRankTmap first.
		TreeMap<Integer, String> stateRankTmap = new TreeMap<Integer, String>(Collections.reverseOrder());

		// Build combined stateRankTmap with aqiStateRankHmap and
		// aqiStateNRefineriesHmap
		for (Entry<String, Long> aqiStateRankEntry : aqiStateRankHmap.entrySet()) {
			String st = aqiStateRankEntry.getKey();
			Long aqiRnk = aqiStateRankEntry.getValue();
			if (aqiStateNRefineriesHmap.containsKey(st)) {
				Integer stateCombinedRank = aqiRnk.intValue() + aqiStateNRefineriesHmap.get(st);
				stateRankTmap.put(stateCombinedRank, st);
			}
		}

		if (!stateRankTmap.isEmpty()) {
			for (Entry<Integer, String> stateRankTmapEntry : stateRankTmap.entrySet()) {
				context.write(new IntWritable(stateRankTmapEntry.getKey()), new Text(stateRankTmapEntry.getValue()));
			}
		}

	}

}
