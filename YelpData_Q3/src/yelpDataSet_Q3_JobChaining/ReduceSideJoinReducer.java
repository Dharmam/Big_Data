package yelpDataSet_Q3_JobChaining;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideJoinReducer extends Reducer<Text, CustomPair, Text, Text> {

	 
   public void reduce(Text key, Iterable<CustomPair> values,Context context) throws IOException, InterruptedException {    
		String dataA_str = "";
		String dataB_str = "";
		
		boolean  flag= false;
		for (CustomPair value : values) {
			if (value.getidentifier().toString().equals("1")) {
				dataA_str = value.getdata().toString();	
				flag=true;
			}  
			else{
				dataB_str = value.getdata().toString();
			}	
		}
		if(!flag){
			return;
		}
			
	
		String data = dataB_str + " " + dataA_str;
		context.write(key, new Text(data));
	   
 }
}
