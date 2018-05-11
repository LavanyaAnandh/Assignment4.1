import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 

public class TvSurveyTask1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|"); //This will get the entire line as array of words by splitting the delimiter '|'
		if((lineArray.length > 0) && 
				(lineArray[0] != null) &&
				(lineArray[1] != null) &&
				(! lineArray[0].equalsIgnoreCase("NA")) &&
				(! lineArray[1].equalsIgnoreCase("NA")) //This condition checks if the 0th and 1st value in array is not 0 or NA
				) {
				context.write(new Text(lineArray[0]), new Text("\t" + lineArray[1])); //This will write the 0th and 1st element in string of array as (comp,\tproduct) 
			}
	}
}
