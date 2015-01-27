package com.jd.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LDAView {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\t");
			int itemLen = items.length;
			if (itemLen < 5) {
				return;
			}
			String userId = items[0];
			String skuId = items[2];

			if (skuId.contains(":"))
				return;
			context.write(new Text(userId), new Text(skuId));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private int docLength = 30;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			docLength = conf.getInt("doc.length", 10);

		}

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

			if (wordCount.size() >= docLength) {
				int wordSize = wordCount.size();
				Iterator<String> keySetIterator = wordCount.keySet().iterator();
				StringBuilder str = new StringBuilder();
				str.append(wordSize + "\t");
				while (keySetIterator.hasNext()) {
					String word = keySetIterator.next();
					str.append(word);
					str.append(":");
					str.append(wordCount.get(word));
					str.append("\t");
				}

				context.write(key, new Text(str.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("doc.length", args[2]);
		conf.set("mapred.child.java.opts", "-Xmx3360m");

		Job job = Job.getInstance(conf);
		job.setJarByClass(LDAView.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
