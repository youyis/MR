package com.jd.tools;

import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.jd.common.*;

public class GetSkuPairByTime {
	public static class ActualKeyPartitioner extends
			Partitioner<CompositeKey, Text> {
		@Override
		public int getPartition(CompositeKey key, Text value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode()) % numPartitions;
		}
	}

	public static class ActualKeyGroupingComparator extends WritableComparator {
		public ActualKeyGroupingComparator() {
			super(CompositeKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable tp1, WritableComparable tp2) {
			CompositeKey Userid2Time1 = (CompositeKey) tp1;
			CompositeKey Userid2Time2 = (CompositeKey) tp2;
			return Userid2Time1.getFirst().compareTo(Userid2Time2.getFirst());
		}
	}

	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(CompositeKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CompositeKey key1 = (CompositeKey) w1;
			CompositeKey key2 = (CompositeKey) w2;

			// (first check on udid)
			int compare = key1.getFirst().compareTo(key2.getFirst());

			if (compare == 0) {
				return key1.getSecond().compareTo(key2.getSecond());
			}

			return compare;
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, CompositeKey, Text> {

		private final HashSet<String> skuSet = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path skuPath = new Path(conf.get("sku.set"));

			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(skuPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] items = line.trim().split("\t");
					skuSet.add(items[0]);
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
			if (items.length < 5) {
				return;
			}

			if (items[1].equals("NULL") || items[2].equals("NULL")) {
				return;
			}

			 String userId = items[0];
			 String skuId = items[1];
			 String time = items[4];

			/*
			String userId = items[0];
			String skuId = items[2];
			if (skuSet.contains(skuId))
				return;
			String time = items[4];
			if (time.compareTo("2014-10-01") < 0)
				return;
			Double rand = Math.random();

			if (rand < 0.5)
				return;
        */
			context.write(new CompositeKey(userId, time), new Text(skuId + "#"
					+ time));
		}
	}

	public static class Reduce extends Reducer<CompositeKey, Text, Text, Text> {
		private int window_size = 14 * 24; // 7 Days

		public static Long timeval(String t) {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			try {
				Date d1 = format.parse(t);
				long Time = d1.getTime();
				return new Long(Time / (1000 * 60 * 60));
			} catch (Exception e) {
				e.printStackTrace();
			}
			return new Long(0);
		}

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			window_size = conf.getInt("Sku.Window.Size", 14 * 24);
		}

		@Override
		public void reduce(CompositeKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			LinkedList<Pair<String, Long>> linkedlist = new LinkedList<Pair<String, Long>>();

			for (Text value : values) {
				String[] sku2time = value.toString().split("#");
				if (sku2time.length < 2)
					continue;

				String sku = sku2time[0];
				String time = sku2time[1];

				Long Day = timeval(time);
				if (Day != 0)
					linkedlist.add(new Pair<String, Long>(sku, Day));
			}

			try {
				do {
					Pair<String, Long> head = linkedlist.removeFirst();
					ListIterator<Pair<String, Long>> itr = linkedlist
							.listIterator(0);
					while (itr.hasNext()) {
						Pair<String, Long> s2t = itr.next();
						if (s2t.getR() - head.getR() > window_size)
							break;
						if (s2t.getL().compareTo(head.getL()) == 0)
							continue;
						context.write(new Text(head.getL()),
								new Text(s2t.getL()));
					}
				} while (true);
			} catch (NoSuchElementException e) {
				// nothing to do
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("sku.set", args[2]);
		conf.set("Sku.Window.Size", args[3]);
		conf.set("mapred.child.java.opts", "-Xmx5120m");

		Job job = Job.getInstance(conf);
		job.setJarByClass(GetSkuPairByTime.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// group and partition by the first int in the pair
		job.setPartitionerClass(ActualKeyPartitioner.class);
		job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(args[4]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
