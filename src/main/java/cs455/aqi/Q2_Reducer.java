package cs455.aqi;

import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2_Reducer extends Reducer<IntWritable, LongWritable, LongWritable, Text> {
	private HashMap<Integer, Long> monthAqiHmap;

	enum monthsEnum {
		January, February, March, April, May, June, July, August, September, October, November, December
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		monthAqiHmap = new HashMap<Integer, Long>();
	}

	@Override
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		Integer month = key.get();
		Long aqi = 0L;

		for (LongWritable val : values) {
			aqi += val.get();
		}

		if (monthAqiHmap.containsKey(month)) {
			Long prevAqi = monthAqiHmap.get(month);
			monthAqiHmap.put(month, aqi + prevAqi);
		} else {
			monthAqiHmap.put(month, aqi);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		String[] monthsList = new DateFormatSymbols().getMonths();
		List<Entry<Integer, Long>> monthAqilist = new ArrayList<>(monthAqiHmap.entrySet());
		monthAqilist.sort(Entry.comparingByValue());
		for (Entry<Integer, Long> entry : monthAqilist) {
			String monthKey = monthsList[entry.getKey()-1];
			Long aqiMonthValue = entry.getValue();
			context.write(new LongWritable(aqiMonthValue), new Text(monthKey));
		}
	}
}