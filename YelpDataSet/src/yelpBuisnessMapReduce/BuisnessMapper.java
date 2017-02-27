package yelpBuisnessMapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BuisnessMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text mapKey = new Text();
	Text mapValue = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] sentence = value.toString().split("::");
		String address = sentence[1];
		String categories = sentence[2];

		String[] categoriesArr = categories.substring(categories.indexOf('(') + 1, categories.indexOf(')')).split(",");

		mapValue.set(address);

		if (mapValue.find("Palo Alto") != -1) {
			for (int i = 0; i < categoriesArr.length; i++) {
				mapKey.set(categoriesArr[i].trim());
				context.write(mapKey , mapValue);
			}
		}

	}

}
