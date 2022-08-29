package cs455.aqi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1_Reducer extends Reducer<IntWritable, LongWritable, LongWritable, Text> {
	private HashMap<Integer, Long> dayAqiHmap;


	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		dayAqiHmap = new HashMap<Integer, Long>();
	}

	@Override
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		Integer day = key.get();
		Long aqi = 0L;

		for (LongWritable val : values) {
			aqi += val.get();
		}

		if (dayAqiHmap.containsKey(day)) {
			Long prevAqi = dayAqiHmap.get(day);
			dayAqiHmap.put(day, aqi + prevAqi);
		} else {
			dayAqiHmap.put(day, aqi);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		String[] daysList = new String[] { "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
		"Saturday" };
		List<Entry<Integer, Long>> dayAqilist = new ArrayList<>(dayAqiHmap.entrySet());
		dayAqilist.sort(Entry.comparingByValue());
		for (Entry<Integer, Long> entry : dayAqilist) {
			String dayKey = daysList[entry.getKey()-1];
			Long aqiDayValue = entry.getValue();
			context.write(new LongWritable(aqiDayValue), new Text(dayKey));
		}
	}
}
