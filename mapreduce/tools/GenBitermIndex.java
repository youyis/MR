package com.jd.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class GenBitermIndex {
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
			int N = items.length;
			if (N < 2) {
				return;
			}

			for (int i = 1; i < N; ++i) {
				String[] terms = items[i].split(":");
				String termL = terms[0];
				String termR = terms[1];

				if (termL.compareTo(termR) > 0) {
					String temp = termL;
					termL = termR;
					termR = temp;
				}

				context.write(new CompositeKey(termL, termR),new IntWritable(1));
			}
		}
	}

	public static class Reduce extends
			Reducer<CompositeKey, IntWritable, Text, IntWritable> {
		private int index = 0; 
		private final HashMap<String, String> termIndex = new HashMap<String, String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path skuPath = new Path(conf.get("term.index"));

			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(skuPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] items = line.split("\t");
					termIndex.put(items[0], items[1]);
					line = br.readLine();
				}
			} catch (Exception e) {
			}
		}

		@Override
		public void reduce(CompositeKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String term_1 = key.getFirst();
			String term_2 = key.getSecond();
			StringBuilder str = new StringBuilder("");
			str.append(termIndex.get(term_1));
			str.append("\t");
			str.append(termIndex.get(term_2));
			
			context.write(new Text(str.toString()), new IntWritable(index++));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("term.index", args[2]);
		conf.set("mapred.child.java.opts", "-Xmx5120m");

		Job job = Job.getInstance(conf);
		job.setJarByClass(GenBitermIndex.class);
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

		job.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
