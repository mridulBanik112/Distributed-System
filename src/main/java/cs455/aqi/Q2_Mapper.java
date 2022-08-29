package cs455.aqi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q2_Mapper extends Mapper<Object, Text, IntWritable, LongWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String dataRowStr = value.toString();
		String dataRowList[] = dataRowStr.split(",");

		if (dataRowList.length == 6) {
			Integer mo = Integer.parseInt(DateUtility.epochTimeToMonth(Long.parseLong(dataRowList[1])));
			Long aqi = Long.parseLong(dataRowList[3]);
			context.write(new IntWritable(mo), new LongWritable(aqi));
		}
	}
}
