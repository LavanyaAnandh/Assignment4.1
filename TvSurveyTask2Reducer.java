import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TvSurveyTask2Reducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
	  int sum = 0;
	  String company = key.toString(); //This will store the key value as company
	  for (IntWritable t : values) {
	  System.out.println("Company=>"+key) ;	//For Logging purpose.
      sum+=t.get();	 //this adds up the each key value. 
       }		
	  context.write(new Text(company), new IntWritable(sum)); //This will write the final output as (company no of times that company pdt sold out)
  }
}

