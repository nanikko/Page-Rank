import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.*;
/*
 * This is a rank initializer class, which initializes a rank 1.0 for each node in the graph using map reduce
 * 
 * */

public class RankInitializer {
/*
 * In this mapper we append rank 1.0 to each node in a specific format and emit node and its edges
 * */
	public static class Map extends Mapper<LongWritable, Text,	Text, Text>{
	      private final static IntWritable one = new IntWritable(1);
	      private Text nodeId = new Text();
 
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        
	        if(tokenizer .hasMoreElements()){
	        	nodeId.set(tokenizer.nextToken());
	        	String withRank = " 1.0 ";
	        	while(tokenizer.hasMoreTokens()){
	        		withRank = withRank+" "+tokenizer.nextToken();
	        	}
	        		
	        	
	        	context.write(nodeId, new Text(withRank));
	        	}
	        
	        
	        
	      }
	    }
	
	/*
	 * In this reducer class we just collect the output from the mapper class and emit it to the output context.
	 * */
	
public static class Reduce extends Reducer<LongWritable, Text, Text, Text> {
    	
    	
      public void reduce(Text key, Text value,Context context) throws IOException, InterruptedException {
    	context.write(key, value);  
    	
        	
      }
    }



/*
 * This is the function which is called from Main method.This method sets the mapper class, reducer class
 * and runs the job. 
 * */


public static void initializeRank(String inputFile,String outputFile) throws Exception {

	Configuration config = new Configuration();
    Job rankInitializerJob = new Job(config,"InitializeRank");
    rankInitializerJob.setOutputKeyClass(Text.class);
    rankInitializerJob.setOutputValueClass(Text.class);

    rankInitializerJob.setMapperClass(Map.class);
   
    rankInitializerJob.setReducerClass(Reduce.class);
    rankInitializerJob.setJarByClass(RankInitializer.class);

    rankInitializerJob.setInputFormatClass(TextInputFormat.class);
    rankInitializerJob.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(rankInitializerJob, new Path(inputFile));
    FileOutputFormat.setOutputPath(rankInitializerJob, new Path(outputFile));

    rankInitializerJob.waitForCompletion(true);
}	
	


}
