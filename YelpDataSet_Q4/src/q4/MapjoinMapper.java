package q4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapjoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	static String total_record = "";
	static HashMap<String, String> map = new HashMap<String, String>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		URI[] files = context.getCacheFiles();

		if (files.length == 0) {
			throw new FileNotFoundException("Distributed cache file not found");
		}
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(files[0]));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		readCacheFile(br);
	};

	private void readCacheFile(BufferedReader br) throws IOException {
		String line = br.readLine();
		while (line != null) {
			String[] fields = line.split("::");
			map.put(fields[0].trim(), fields[1].trim());
			line = br.readLine();
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		Text mapKey = new Text();
		Text mapValue = new Text();

		String[] sentence = value.toString().split("::");
		String buisnessId = sentence[2].trim();
		String userId = sentence[1].trim();
		Float rating = Float.parseFloat(sentence[3]);

		if (map.containsKey(buisnessId)) {
			mapValue.set(Float.toString(rating));
			mapKey.set(userId.trim());
			context.write(mapKey, mapValue);
		}

	}

}
