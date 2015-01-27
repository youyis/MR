package com.jd.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LDAToIndex {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private final HashMap<String, String> skuIndex = new HashMap<String, String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path skuPath = new Path(conf.get("skuid.index"));

			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(skuPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] items = line.split("\t");
					if (items.length < 2)
						continue;
					skuIndex.put(items[0], items[1]);
					line = br.readLine();
				}
			} catch (Exception e) {
			}

		}

		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {

			String line = inValue.toString();
			String[] items = line.split("\t");
			int count = Integer.parseInt(items[1]);
			StringBuilder str = new StringBuilder("");
			for (int i = 2; i < items.length; i++) {
				String[] skuCount = items[i].split(":");
				if (skuCount.length < 2) {
					count -= 1;
					continue;
				}

				if (!skuIndex.containsKey(skuCount[0])) {
					count -= 1;
					continue;
				}

				String index = skuIndex.get(skuCount[0]);
				str.append(index + ":" + skuCount[1]);
				str.append(" ");
			}
			String lineOut = str.subSequence(0, str.length() - 1).toString();
			context.write(new Text(items[0]), new Text(count + " " + lineOut));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("skuid.index", args[2]);
		conf.set("mapred.child.java.opts", "-Xmx5120m");

		Job job = Job.getInstance(conf);
		job.setJarByClass(LDAToIndex.class);
		job.setMapperClass(Map.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
