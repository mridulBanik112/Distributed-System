package cs455.aqi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q6 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] remArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (remArgs.length < 3) {
			System.err.println("Please provide 2 input paths (aqi and refinery) and an output path.");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Highest Combined AQI and Refineries By State");
		job.setJarByClass(Q6.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Q6_Aqi_Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Q6_nRefineries_Mapper.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		//job.setMapperClass(Highest_state_combined_aqi_mapper.class);
		job.setReducerClass(Q6_Reducer.class);
		
		FileSystem.get(conf).delete(new Path(args[2]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}