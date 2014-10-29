import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
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
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.*;

/*
 * 
 * In this class we calculate page rank using the mapreduce. 
 * 
 * */

public class PageRank {
	
	public static enum CONVERGENCE_CHECK {
		convergenceCheck;
	};
	
	/*
	 * In this mapper method we emit 2 things
	 * 	1) edgeNodeId with its partial rank
	 * 	2) NodeId with its oldRanka and edges List
	 * 
	 * */
	
	public static class Map extends Mapper<LongWritable, Text,	Text, Text>{
	      private final static IntWritable one = new IntWritable(1);
	      private Text nodeId = new Text();
	      private Text edgeId = new Text();
 
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        
	        if(tokenizer .hasMoreElements()){
	        	nodeId.set(tokenizer.nextToken());
	        	float nodeIdRank = Float.parseFloat(tokenizer.nextToken());
	        	int noOfEdges = tokenizer.countTokens();
	        	float edgeRank =nodeIdRank/noOfEdges;
	        	String edges=Float.toString(nodeIdRank)+" ";
	        	while(tokenizer.hasMoreTokens()){
	        		String edge =tokenizer.nextToken();
	        		edgeId.set(edge);
	        		edges+=edge+" ";
	        		context.write(edgeId,new Text(Float.toString(edgeRank)) );
	        	}
	        	edges+="$$$";
	        	context.write(nodeId, new Text(edges));
	        		
	        	}
	        
	        
	        
	      }
	    }
	
	
	/*
	 * In this reduce method we accumulated all the partial ranks apply damping factor formula to calculate new rank. 
	 * We write node with its new rank and edge list
	 * */
	
	
public static class Reduce extends Reducer<Text, Text, Text, Text> {
  	
  	
    public void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
    	float sum = 0;
    	String edges="";
    	String k = key.toString();
    	float oldRank=0;
    	float newRank=0;
    	Configuration conf = context.getConfiguration();		
		String df = conf.get("dampFactor");
    	float dampFactor = Float.parseFloat(df);
    for(Text t:values){
       
    	   String value= t.toString();
          if(!value.endsWith("$$$"))
        	  sum += Float.parseFloat(value);
          else {
        	  String s = value.substring(0, value.length()-3);
        	  String[] str=s.split(" ", 2);
        	  edges=str[1];
        	  oldRank=Float.parseFloat(str[0]);
        	  
          }
    }
      newRank = (1-dampFactor)+ dampFactor*sum;
    
    if ((Math.abs(newRank - oldRank) > 0.001)) {
		context.getCounter(CONVERGENCE_CHECK.convergenceCheck).increment(
				1);
	}
    
    
      context.write(key, new Text(Float.toString(newRank)+ " "+edges));
 
    }
  }

/*
 * This is the function which is called from Main method.This method sets the mapper class, reducer class
 * and runs the job. 
 * */
public static long[] CalculateRank(String inputFile,String outputFile,String dampingFactor) throws Exception {

	Configuration config = new Configuration();
  Job rankCalculationJob = new Job(config,"PageRank");
  rankCalculationJob.getConfiguration().set("dampFactor", dampingFactor);
  rankCalculationJob.setOutputKeyClass(Text.class);
  rankCalculationJob.setOutputValueClass(Text.class);

  rankCalculationJob.setMapperClass(Map.class);
  
  rankCalculationJob.setReducerClass(Reduce.class);
rankCalculationJob.setJarByClass(PageRank.class);
  rankCalculationJob.setInputFormatClass(TextInputFormat.class);
  rankCalculationJob.setOutputFormatClass(TextOutputFormat.class);

  FileInputFormat.addInputPath(rankCalculationJob, new Path(inputFile));
  FileOutputFormat.setOutputPath(rankCalculationJob, new Path(outputFile));
  rankCalculationJob.waitForCompletion(true);
  Counters counters = rankCalculationJob.getCounters();
  Counter finalConvergence = counters.findCounter(CONVERGENCE_CHECK.convergenceCheck);

 
  long[] a = {finalConvergence.getValue()};
  return a;
}	
	

	

}
