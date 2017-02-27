package yelpDataSet_Q3_JobChaining;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BuisnessMapper extends Mapper<LongWritable, Text,Text, CustomPair> {

	Text mapKey = new Text();
	CustomPair mapValue = new CustomPair();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	String[] sentence = value.toString().split("::");
	String buisnessId = sentence[0];
	String address = sentence[1];
	String categories = sentence[2];

	StringBuilder finalValue = new StringBuilder();
	finalValue.append(address);
	finalValue.append(" ");
	finalValue.append(categories);
	
	mapKey.set(buisnessId);
	mapValue.setdata(new Text(finalValue.toString()));
	mapValue.setidentifier(new Text("2"));

	context.write(mapKey, mapValue);

	}

}
