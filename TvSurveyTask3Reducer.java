import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TvSurveyTask3Reducer
  extends Reducer<Text, Text, Text, Text> {
  
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
	  int sum = 0;
	  String state = key.toString(); //This will store the key value as company
	  for (Text t : values) {
		  String parts[] = t.toString().split("\t"); //This will split the delimiter \t for each values of a key.
		  System.out.println("Company=>"+key) ;	//For Logging purpose.
      	  sum++;	
       }
	  String str = String.format("%d", sum);
	  context.write(new Text(state), new Text(str)); //This will write the final output as (State how many pdt sold in that state)
  }
}

