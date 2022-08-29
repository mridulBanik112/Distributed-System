package cs455.aqi;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5_Reducer extends Reducer<Text, LongWritable, LongWritable, Text> {
	private TreeMap<Long, String> tmap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		tmap = new TreeMap<Long, String>(Collections.reverseOrder());
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// county : best aqi one week change
		String county = key.toString();
		Long aqi = 0L;

		for (LongWritable val : values) {
			aqi += val.get();
		}
		
		tmap.put(aqi, county);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<Long, String> entry : tmap.entrySet()) {
			Long bestOneWeekAqiChange = entry.getKey();
			String county = entry.getValue();
			context.write(new LongWritable(bestOneWeekAqiChange), new Text(county));
		}
	}

}