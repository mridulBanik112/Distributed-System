package cs455.aqi;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q4_Mapper extends Mapper<Object, Text, Text, LongWritable> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String dataRowStr = value.toString();
		String dataRowList[] = dataRowStr.split(",");

		if (dataRowList.length == 6) {
			String yr = DateUtility.epochTimeToYear(Long.parseLong(dataRowList[1]));
			if (yr.compareTo("2020") == 0) {
				Long aqi = Long.parseLong(dataRowList[3]);
				String countyState = dataRowList[4] + ", " +  dataRowList[5];
				context.write(new Text(countyState), new LongWritable(aqi));
			}
		}
	}
}