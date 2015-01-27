package com.jd.tools;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntegerComment {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\t");
			int itemLen = items.length;
			if (itemLen < 2) {
				return;
			}

			String skuId = items[0];

			context.write(new Text(skuId), new Text(line));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/*
		 * private int threshold = 0;
		 * 
		 * @Override public void setup(Context context) throws IOException,
		 * InterruptedException { super.setup(context); Configuration conf =
		 * context.getConfiguration(); threshold =
		 * conf.getInt("comment.threshold", 0); }
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			StringBuilder str = new StringBuilder("");
			for (Text value : values) {
				String[] items = value.toString().split("\t");
				int itemLen = items.length;
				count += itemLen - 1;
				for (int i = 1; i < itemLen; i++) {
					str.append(items[i]);
					str.append("\t");
				}
			}

			String line = str.subSequence(0, str.length() - 1).toString();
			context.write(key, new Text(count + "\t" + line));

			/*
			 * if (count < threshold) return; ListIterator<String> iter =
			 * linkedlist.listIterator(); while(iter.hasNext()){
			 * 
			 * context.write(key, new Text(iter.next())); }
			 */
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("comment.threshold", args[2]);
		Job job = Job.getInstance(conf);
		job.setJarByClass(IntegerComment.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
