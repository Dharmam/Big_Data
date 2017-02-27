package q4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BuisnessReducer extends Reducer<Text, Text, Text, Text> {

	private Text finalValue = new Text("Stanford Based Buisness");

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(key, finalValue);
	}
}