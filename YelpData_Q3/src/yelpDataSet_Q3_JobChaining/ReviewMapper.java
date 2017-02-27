package yelpDataSet_Q3_JobChaining;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text mapKey = new Text();
	IntWritable mapValue = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] sentence = value.toString().split("::");
		String buisnessId = sentence[2];
		Float rating = Float.parseFloat(sentence[3]);

		mapValue.set(rating.intValue());
		mapKey.set(buisnessId);
		context.write(mapKey, mapValue);

	}

}
