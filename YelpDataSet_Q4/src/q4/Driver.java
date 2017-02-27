package q4;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {

	final static String INTERMEDIATE_FILE_PATH = "intermediate_output_Map";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", "::");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


		if (otherArgs.length != 3) {
			System.err.println("Incorrect parameter length. Usage <input> <input> <output>");
			System.exit(2);
		}

		Path intermidiateFile=new Path(INTERMEDIATE_FILE_PATH);
		Path inputFileBuisness=new Path(otherArgs[0]);
		Path inputFileReview=new Path(otherArgs[1]);
		Path outputFile=new Path(otherArgs[2]);
		
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "Gets the buisness Id for the stanford based buisnesses.");
		job1.setJarByClass(Driver.class);
		
		
		job1.setMapperClass(BuisnessMapper.class);
		job1.setReducerClass(BuisnessReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, inputFileBuisness);
		FileOutputFormat.setOutputPath(job1, intermidiateFile);
		 
		if (job1.waitForCompletion(true)) {

			@SuppressWarnings("deprecation")
			Job job2=new Job(conf,"Joiner");
			job2.setJarByClass(Driver.class);

			job2.setMapperClass(MapjoinMapper.class);
			
			//job2.setNumReduceTasks(0);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.addCacheFile(new URI(intermidiateFile.getName()+"/part-r-00000"));
			
			FileInputFormat.addInputPath(job2, inputFileReview);
			FileOutputFormat.setOutputPath(job2, outputFile);
			
			//job2.setReducerClass(MapjoinReducer.class);
			
			job2.waitForCompletion(true);

		}
	}

}

