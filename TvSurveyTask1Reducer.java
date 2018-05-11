import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TvSurveyTask1Reducer
  extends Reducer<Text, Text, Text, Text> {
  
  public void reduce(Text key, Iterable<Text> values,
      Context context) throws IOException, InterruptedException {
  	String company = key.toString(); //This will store the key value as company
  	String product = "";
      
	  for (Text t : values) {
			String parts[] = t.toString().split("\t"); //This will split the delimiter \t for each values of a key.
			product = parts[1]; //This will store the 1st element in the array of strings as product after the above splitting
      System.out.println("Company=>"+key) ;	 //For logging purpose
      context.write(new Text(company),new Text(product));	//This will write the final output as (company product)
       }		
  }
}

