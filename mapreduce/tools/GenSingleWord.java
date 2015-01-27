package com.jd.tools;

import java.io.IOException;

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

public class GenSingleWord {
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String[] items = inValue.toString().split("\t");
			int N = items.length;
			if (N < 2) {
				return;
			}
			
			for (int i = 1; i < N; ++i) {
				String[] terms = items[i].split(":");
				String termL = terms[0];
				String termR = terms[1];

				context.write(new Text(termL), new IntWritable(1));
				context.write(new Text(termR), new IntWritable(1));
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, Text> {
		private int index = 0; 
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count = count + value.get();
			}
			
			StringBuilder str = new StringBuilder("");
			str.append(index);
			str.append("\t");
			str.append(count);
			
			index = index + 1;
			
			String lineOut = str.toString();
			context.write(key, new Text(lineOut));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(GenSingleWord.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
