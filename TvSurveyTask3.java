import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TvSurveyTask3 {	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		if(args.length!=2) //creating conf and job parameter needed for execution.
		{
			System.out.println("Tv Survey: <input path> <outputpath>");
			System.exit(-1);
		}
		//creating conf and job parameter needed for execution.
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Tv Survey");
		
		//Setting Driver class and number of reducer to be used.
		job.setJarByClass(TvSurveyTask3.class);
		job.setNumReduceTasks(1);
		
		//Setting i/p path from argument 0.
		FileInputFormat.setInputPaths(job, args[0]);
		
		//Setting Mapper and reducer class that need to be called for execution
		job.setMapperClass(TvSurveyTask3Mapper.class);
		job.setReducerClass(TvSurveyTask3Reducer.class);
				
		System.out.println("Before setting outputkey and class"); //for logging purpose.
		//Setting final output key and value class from reducer.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Setting i/p and o/p format class.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting i/p path from argument 1.		
		Path outputpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputpath);
		outputpath.getFileSystem(conf).delete(outputpath, true); //This will delete/overwrite the output file each time.
		
		System.out.println("After setting outputkey and class"); //for logging purpose.
		System.exit(job.waitForCompletion(true) ? 0:1); //This will start the job execution
	}
	}


