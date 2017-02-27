package yelpDataSet_Q3_JobChaining;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {

	final static String INTERMEDIATE_FILE_PATH = "intermediate_output";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", "::");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


		if (otherArgs.length != 3) {
			System.err.println("Incorrect parameter length. Usage <input> <input> <output>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "Driver for getting buisness ID of top 10 average ratings.");
		job1.setJarByClass(Driver.class);

		job1.setMapperClass(ReviewMapper.class);
		job1.setReducerClass(ReviewReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(INTERMEDIATE_FILE_PATH));
		 
		if (job1.waitForCompletion(true)) {

			@SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "Map reduce on buisness dataset to get all buisness based out of Palo alto.");
			job2.setJarByClass(Driver.class);
			
			job2.setMapOutputValueClass(CustomPair.class);
			job2.setMapOutputKeyClass(Text.class);
			

			job2.setReducerClass(ReduceSideJoinReducer.class);
		
			MultipleInputs.addInputPath(job2, new Path(INTERMEDIATE_FILE_PATH), TextInputFormat.class,
					SortedReviewMapper.class);
			MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, BuisnessMapper.class);
			

			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}

}
