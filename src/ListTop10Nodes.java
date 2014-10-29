import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * In this class we use map reduce to list the top 10 nodes based on the new ranks assigned to the nodes list
 * 
 * */
public class ListTop10Nodes {
	/*
	 * 
	 * In this mapper method we emit node and its rank with a text as its key  
	 * 
	 * */
	public static class Map extends Mapper<LongWritable, Text,	Text, Text>{
	      private final static IntWritable one = new IntWritable(1);
	      private Text nodeIdAndRank = new Text("nodes and Ranks");
	      private Text edgeId = new Text();
 
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        String nodeAndRank="";
	        if(tokenizer .hasMoreElements()){
	        	nodeAndRank =tokenizer.nextToken();
	        	nodeAndRank+=" "+Float.parseFloat(tokenizer.nextToken()); 
	        	
	        	context.write(nodeIdAndRank, new Text(nodeAndRank));
	        		
	        	}
	        
	        
	        
	      }
	    }
	
	/*
	 * In this reduce method we sort the ranks and return the top 10 to the output file
	 * 
	 * */
	
public static class Reduce extends Reducer<Text, Text, Text, Text> {
	
	
  public void reduce(Text key, Iterable<Text>values,Context context) throws IOException, InterruptedException {
  	
	  SortedMap<Float, String> nodesAndRanks= new TreeMap<Float, String>(); 
	  

	  for(Text t:values ){
		String s = t.toString();
		String[] str=s.split(" ", 2);
		nodesAndRanks.put(Float.parseFloat(str[1]), str[0]);
		  
	  }
	  for(int i=0;i<10;i++){
		  Float l = nodesAndRanks.lastKey();
		  context.write(new Text(nodesAndRanks.get(l)),new Text(Float.toString(l)) );
		  nodesAndRanks.remove(l);
		  
		  
		  
	  }
	  
  }
}

/*
 * This is the function which is called from Main method.This method sets the mapper class, reducer class
 * and runs the job. 
 * */
public static void listTop10Nodes(String inputFile,String outputFile) throws Exception {

	Configuration config = new Configuration();
Job listtop10nodes = new Job(config,"ListTop10");

listtop10nodes.setOutputKeyClass(Text.class);
listtop10nodes.setOutputValueClass(Text.class);

listtop10nodes.setMapperClass(Map.class);

listtop10nodes.setReducerClass(Reduce.class);
listtop10nodes.setJarByClass(ListTop10Nodes.class);
listtop10nodes.setInputFormatClass(TextInputFormat.class);
listtop10nodes.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(listtop10nodes, new Path(inputFile));
FileOutputFormat.setOutputPath(listtop10nodes, new Path(outputFile));
listtop10nodes.waitForCompletion(true);

}	
	
	

}
