package com.jd.tools;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterUser {
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String[] items = inValue.toString().split("\t");
			if (items.length < 5) {
				return;
			}

			String userId = items[0];
			context.write(new Text(userId), inValue);
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, NullWritable, Text> {
		private int upper_threshold = 10000;
		private int lower_threshold = 1;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			upper_threshold = conf.getInt("user.upper_threshold", 10000);
			lower_threshold = conf.getInt("user.lower_threshold", 1);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			LinkedList<String> linkedlist = new LinkedList<String>();
			for (Text value : values) {
				count = count + 1;
				if(count > upper_threshold) {
					break;
				}
				linkedlist.add(value.toString());
			}
			
			if(count <= lower_threshold || count >= upper_threshold) {
				return;
			}
			
			ListIterator<String> itr = linkedlist.listIterator(0);
			while (itr.hasNext()) {
				context.write(NullWritable.get(), new Text(itr.next()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("user.upper_threshold", args[2]);
		conf.set("user.lower_threshold", args[3]);
		conf.set("mapred.child.java.opts","-Xmx5120m");

		Job job = Job.getInstance(conf);
		job.setJarByClass(FilterUser.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(200);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
