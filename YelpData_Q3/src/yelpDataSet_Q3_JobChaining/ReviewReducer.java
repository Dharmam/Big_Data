package yelpDataSet_Q3_JobChaining;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Map<Text, Text> countMap = new HashMap<>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		Double sum = 0.0;
		int count = 0;
		for (IntWritable value : values) {
			sum += value.get();
			count++;
		}
		Text finalValue = new Text(Double.toString(sum / count));
		countMap.put(new Text(key), finalValue);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Map<Text, Text> sortedMap = sortByValues(countMap);
		
		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == 10) {
				break;
			}
			context.write(key, sortedMap.get(key));
		}
	}

	public static Map<Text, Text> sortByValues(Map<Text, Text> map) {
		List<Map.Entry<Text, Text>> entries = new LinkedList<>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<Text, Text>>() {

			@Override
			public int compare(Entry<Text, Text> o1, Entry<Text, Text> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}

		});

		Map<Text, Text> sortedMap = new LinkedHashMap<Text, Text>();

		for (Map.Entry<Text, Text> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

}
