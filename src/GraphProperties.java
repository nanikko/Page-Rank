import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

/*
 * In this class we use map reduce to find properties of the input graph
 * 
 * */
public class GraphProperties {
	/*
	 * This map function emits a text "NodeOutDegree" with outDegree of the nodes as value 
	 * 
	 * */
    public static class Map extends Mapper<LongWritable, Text,	Text, Text> {
      private final static IntWritable one = new IntWritable(1);
      private Text outdegree = new Text("NodeOutDegree");
 
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        StringTokenizer tokenizer = new StringTokenizer(line);
        if(tokenizer .hasMoreElements()){
        	
        		context.write(outdegree,new Text(Integer.toString(tokenizer.countTokens()-1)) );
        		
        	}
        
        
        
      }
    }
 /*
  * This reduce method collects all the out degree the map method has emitted and performs operations to find minimum,maximum,
  * average,number of edges, number of nodes.
  * 
  * */ 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
    	
    	
      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	int minOutDegree=Integer.MAX_VALUE;
      	int maxOutDegree=0;
      	int count=0;
      	float avgOutDegree;
      	boolean f=false;
        int sum = 0;
        String k = key.toString();
        
        for(Text value:values){
        	int tmp = Integer.parseInt(value.toString());
        	if(tmp<minOutDegree)
        		minOutDegree=tmp;
        	if(tmp>maxOutDegree)
        		maxOutDegree=tmp;
        	sum +=tmp;
        	count++;
        	
        }
        
        avgOutDegree=sum/count;
        context.write(new Text("Number Of Edges"), new Text(Integer.toString(sum)));
    	context.write(new Text("Number Of nodes"), new Text(Integer.toString(count)));
    	context.write(new Text("Min Out Degree"), new Text(Integer.toString(minOutDegree)));
    	context.write(new Text("Max Out Degree"), new Text(Integer.toString(maxOutDegree)));
    	context.write(new Text("Avg Out Degree"), new Text(Float.toString(avgOutDegree)));

        	
      }
    }
    
    /*
     * This is the function which is called from Main method.This method sets the mapper class, reducer class
     * and runs the job. 
     * */
    
    public static void checkGraphProperties(String inputFile,String outputFile) throws Exception {
  	Configuration config = new Configuration();
      Job findGraphProperties = new Job(config,"GraphProperties");
      findGraphProperties.setOutputKeyClass(Text.class);
      findGraphProperties.setOutputValueClass(Text.class);

      findGraphProperties.setMapperClass(Map.class);
     
      findGraphProperties.setReducerClass(Reduce.class);
      findGraphProperties.setJarByClass(GraphProperties.class);

      findGraphProperties.setInputFormatClass(TextInputFormat.class);
      findGraphProperties.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(findGraphProperties, new Path(inputFile));
      FileOutputFormat.setOutputPath(findGraphProperties, new Path(outputFile));

      findGraphProperties.waitForCompletion(true);
  }	
    
}
