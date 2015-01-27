package com.jd.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jd.common.CompositeKey;

public class CountSkuPair {
	public static class ActualKeyPartitioner extends
			Partitioner<CompositeKey, IntWritable> {
		@Override
		public int getPartition(CompositeKey key, IntWritable value,
				int numPartitions) {
			return Math.abs(Math.abs(key.getFirst().hashCode()
					+ key.getSecond().hashCode())
					% numPartitions);
		}
	}

	public static class ActualKeyGroupingComparator extends WritableComparator {
		public ActualKeyGroupingComparator() {
			super(CompositeKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable tp1, WritableComparable tp2) {
			CompositeKey sku2sku1 = (CompositeKey) tp1;
			CompositeKey sku2sku2 = (CompositeKey) tp2;
			return sku2sku1.compareTo(sku2sku2);
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, CompositeKey, IntWritable> {
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String[] items = inValue.toString().split("\t");
			if (items.length < 2) {
				return;
			}
			String skuIdL = items[0];
			String skuIdR = items[1];

			Double rand = Math.random();
			if (rand < 0.5)
				return;

			if (skuIdL.compareTo(skuIdR) > 0) {
				String temp = skuIdL;
				skuIdL = skuIdR;
				skuIdR = temp;
			}

			context.write(new CompositeKey(skuIdL, skuIdR), new IntWritable(1));
		}
	}

	public static class Reduce extends
			Reducer<CompositeKey, IntWritable, Text, IntWritable> {
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@Override
		public void reduce(CompositeKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count = count + value.get();
			}
			context.write(new Text(key.toString()), new IntWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		conf.set("mapred.child.java.opts", "-Xmx3360m");
		job.setJarByClass(CountSkuPair.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// group and partition by the first int in the pair
		job.setPartitionerClass(ActualKeyPartitioner.class);
		job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
