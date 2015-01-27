package com.jd.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SessionSku {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\t");
			int itemLen = items.length;
			if (itemLen < 5) {
				return;
			}
			String session = items[1];
			String[] sessionItems = session.split("\\|");
	
			String skuId = items[2];

			context.write(new Text(sessionItems[0]), new Text(skuId));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

			for (Text value : values) {
				String word = value.toString();
				if (wordCount.containsKey(word)) {
					wordCount.put(word, wordCount.get(word) + 1);
				} else {
					wordCount.put(word, 1);
				}
			}
	
				int wordSize = wordCount.size();


				context.write(key, new IntWritable(wordSize));			
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapred.child.java.opts", "-Xmx5120m");

		Job job = Job.getInstance(conf);
		job.setJarByClass(SessionSku.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
