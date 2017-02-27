package yelpDataSet_Q3_JobChaining;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SortedReviewMapper extends Mapper<LongWritable, Text, Text, CustomPair> {
	Text mapKey = new Text();
	CustomPair mapValue = new CustomPair();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] sentence = value.toString().split("::");
		String buisnessId = sentence[0];
		String ratings = sentence[1];

		mapKey.set(buisnessId);
		mapValue.setdata(new Text(ratings));
		mapValue.setidentifier(new Text("1"));
		context.write(mapKey, mapValue);
	}
}