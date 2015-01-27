package com.jd.tools;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class skuUserCount{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	     
	  //   private final static IntWritable one = new IntWritable(1);

	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       String line = value.toString();
	       String[] items = line.split("\t");
	       if(items.length < 5){
	    	   return;
	       }
	       
	 //      String userId = items[0];
	     String Id = items[2];
	//      output.collect(new Text(userId), new Text(skuId));	

	   //    int count = Integer.parseInt(items[2]);
	       output.collect(new Text(Id), new IntWritable(1));
	     }
	   }	 	
 
	   public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {	 	       
	 	       int sum = 0;
	 	       while (values.hasNext()) {
	 	         sum += values.next().get();
	 	       }
	 	       output.collect(key, new IntWritable(sum));
	     }
	   }
	
	   
	   
	   public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(skuUserCount.class);
	     conf.setJobName("lda");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setMapOutputKeyClass(Text.class);
	     conf.setMapOutputValueClass(IntWritable.class);
	     conf.setOutputValueClass(IntWritable.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	     conf.setNumReduceTasks(Integer.parseInt(args[2]));

	     JobClient.runJob(conf);
	   }

}
