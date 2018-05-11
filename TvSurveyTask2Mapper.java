import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 


public class TvSurveyTask2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|"); //This will get the entire line as array of words by splitting the delimiter '|'
		IntWritable val = new IntWritable(1);
		if((lineArray.length > 0) && 
				(lineArray[0] != null) &&
				(! lineArray[0].equalsIgnoreCase("NA")) //This condition checks if the 0th element in array is not 0 or NA
				) {
				context.write(new Text(lineArray[0]),val); //This will write the 0th in string of array and add 1 next to it as (comp,1)
			}
	}
}
