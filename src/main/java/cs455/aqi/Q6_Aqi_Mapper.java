package cs455.aqi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q6_Aqi_Mapper extends Mapper<Object, Text, Text, Text> {
	private HashMap<String, Long> stateAqiHmap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		stateAqiHmap = new HashMap<String, Long>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String dataRowStr = value.toString();
		String dataRowList[] = dataRowStr.split(",");
		Long aqi = Long.parseLong(dataRowList[3]);
		String state = dataRowList[5];
		
		if (stateAqiHmap.containsKey(state)) {
			Long prevAqi = stateAqiHmap.get(state);
			stateAqiHmap.put(state, prevAqi+aqi);
		} else {
			stateAqiHmap.put(state, aqi);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		List<Entry<String, Long>> stateAqiList = new ArrayList<>(stateAqiHmap.entrySet());
		stateAqiList.sort(Entry.comparingByValue());
		Integer aqiRank = 0;
		Long lastValue = 0L;
		
		// Write(state, aqi;aqiRank
		for (Entry<String, Long> entry : stateAqiList) {
			String st = entry.getKey();
			Long stateTotalAqi = entry.getValue();
			
			if (stateTotalAqi == lastValue) {
				context.write(new Text(st), new Text("aqi;" + aqiRank.toString()));
			} else {
				aqiRank++;
				context.write(new Text(st), new Text("aqi;" + aqiRank.toString()));
			}
			lastValue = stateTotalAqi;
		}
	}
}
