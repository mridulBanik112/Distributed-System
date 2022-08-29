package cs455.aqi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q6_nRefineries_Mapper extends Mapper<Object, Text, Text, Text> {
	// <State, nRefineries>
	private HashMap<String, Integer> stateRefineriesHmap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		stateRefineriesHmap = new HashMap<String, Integer>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String dataRowStr = value.toString();
		String dataRowList[] = dataRowStr.split(",");
		String status = dataRowList[2];
		String state = dataRowList[1];
		
		// Add nRefineries as long as it's in service for a state.
		if (stateRefineriesHmap.containsKey(state) && status.equals("IN SERVICE")) {
			Integer prevNRefineries = stateRefineriesHmap.get(state);
			stateRefineriesHmap.put(state, prevNRefineries++);
		} 
		else if (status.equals("IN SERVICE")) {
			stateRefineriesHmap.put(state, 1);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		List<Entry<String, Integer>> stateRefineriesList = new ArrayList<>(stateRefineriesHmap.entrySet());
		stateRefineriesList.sort(Entry.comparingByValue());
		Integer nRefineriesRank = 0;
		Integer lastValue = 0;
		
		// Write(state, refineries|nRefineriesRank
		for (Entry<String, Integer> entry : stateRefineriesList) {
			String st = entry.getKey();
			Integer stateTotalRefineries = entry.getValue();
			
			if (stateTotalRefineries == lastValue) {
				context.write(new Text(st), new Text("refineries;" + nRefineriesRank.toString()));
			} else {
				nRefineriesRank++;
				context.write(new Text(st), new Text("refineries;" + nRefineriesRank.toString()));
			}
			lastValue = stateTotalRefineries;
		}
	}
}
