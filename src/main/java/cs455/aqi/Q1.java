package cs455.aqi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] remArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (remArgs.length < 2) {
			System.err.println("Please provide an input path and an output path.");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Best Worst Day AQI");
		job.setJarByClass(Q1.class);

		job.setMapperClass(Q1_Mapper.class);
		job.setReducerClass(Q1_Reducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
