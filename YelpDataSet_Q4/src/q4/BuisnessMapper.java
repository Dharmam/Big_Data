package q4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BuisnessMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text mapKey = new Text();
	Text mapValue = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] sentence = value.toString().split("::");
		String buisnessId = sentence[0].trim();
		String address = sentence[1];

		mapValue.set(address);
		
		if (mapValue.find("Stanford") != -1) {
				mapKey.set(buisnessId);
				context.write(mapKey , mapValue);
			}
		

	}

}
