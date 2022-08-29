package cs455.aqi;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q5_Mapper extends Mapper<Object, Text, Text, LongWritable> {
	private TreeMap<String, TreeMap<Integer, Long>> countyStateTmap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		countyStateTmap = new TreeMap<String, TreeMap<Integer, Long>>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String dataRowStr = value.toString();
		String dataRowList[] = dataRowStr.split(",");

		if (dataRowList.length == 6) {
			Integer wk = Integer.parseInt(DateUtility.epochTimeToWeek(Long.parseLong(dataRowList[1])));
			Long aqi = Long.parseLong(dataRowList[3]);
			String countyState = dataRowList[4] + ", " +  dataRowList[5];

			if (countyStateTmap.containsKey(countyState)) {
				// Add aqi if week key exists
				if (countyStateTmap.get(countyState).containsKey(wk)) {
					Long countyStateTmapPrevAqi = countyStateTmap.get(countyState).get(wk);
					countyStateTmap.get(countyState).put(wk, countyStateTmapPrevAqi + aqi);
				} else {
					countyStateTmap.get(countyState).put(wk, aqi);
				}
			} else {
				TreeMap<Integer, Long> newcountyStateTmap = new TreeMap<Integer, Long>();
				newcountyStateTmap.put(wk, aqi);
				countyStateTmap.put(countyState, newcountyStateTmap);
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<String, TreeMap<Integer, Long>> entry : countyStateTmap.entrySet()) {
			String county = entry.getKey();
			Long cleanupAqi = getBestOneWeekAqChange(entry.getValue());
			context.write(new Text(county), new LongWritable(cleanupAqi));
		}
	}

	// Get best aqi by week for TreeMap<week, aqi>.
	public static Long getBestOneWeekAqChange(TreeMap<Integer, Long> bestOneWkTmap) {
		Long bestAqi = 0L;
		Long prevAqi = null;
		Integer prevWk = null;

		for (Entry<Integer, Long> entry : bestOneWkTmap.entrySet()) {
			Integer bestOneWkTmapWeek = entry.getKey();
			Long bestOneWkTmapAqi = entry.getValue();

			if (prevWk == null) {
				bestAqi = bestOneWkTmapAqi;
				prevWk = bestOneWkTmapWeek;
				prevAqi = bestOneWkTmapAqi;
			} else if (prevWk == bestOneWkTmapWeek) {
				prevAqi += bestOneWkTmapAqi;
				if (prevAqi > bestAqi) {
					bestAqi = prevAqi;
				}
			} else if (prevWk < bestOneWkTmapWeek) {
				prevWk = bestOneWkTmapWeek;
				prevAqi = bestOneWkTmapAqi;
				if (prevAqi > bestAqi) {
					bestAqi = prevAqi;
				}
			}
		}

		return bestAqi;
	}

}
