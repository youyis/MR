package com.jd.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.Math;

public class SampleSkuPair {
	public static class Map extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		private double probability = 0.01;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			probability = conf.getDouble("pair.SampleProbability", 0.01);
		}
		
		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			double p = Math.random();
			if (p > probability) {
				return ;
			}
			context.write(NullWritable.get(), inValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("pair.SampleProbability", args[2]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(SampleSkuPair.class);
		job.setMapperClass(Map.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
