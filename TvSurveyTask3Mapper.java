import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 


public class TvSurveyTask3Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String val = "Onida";
		String[] lineArray = value.toString().split("\\|"); //This will get the entire line as array of words by splitting the delimiter '|'
		System.out.println("Company=>" + lineArray[0]) ; //for logging purpose
		if (lineArray[0].equals(val)) //This checks if the 0th element contains Onida.
		{
			context.write(new Text(lineArray[3]), new Text(lineArray[0])); //This writes the 3rd element and 0th element as (kerala , Onida)
		}
		}
	}

