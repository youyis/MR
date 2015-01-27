package com.jd.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;

import org.apache.hadoop.hbase.util.Bytes;

public class putNumericVectorBulk {
	public static class Map extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		private String columnFamily = "";
		private String columnName = "";

		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			columnFamily = conf.get("column.Family");
			columnName = conf.get("column.name");
		}

		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\t");
			if (items.length < 2) {
				return;
			}

			String Id = items[0];
			Put putRow = new Put(Id.getBytes());
			String[] vec = items[1].split(" ");
			int len = vec.length;
			if (len == 1) {
				putRow.add(columnFamily.getBytes(), (columnName).getBytes(),
						Bytes.toBytes(Float.parseFloat(items[1])));
			} else {
				for (int i = 0; i < len; i++) {
					putRow.add(columnFamily.getBytes(),
							(columnName + (i + 1)).getBytes(),
							Bytes.toBytes(Float.parseFloat(vec[i])));
				}
			}

			ImmutableBytesWritable HKey = new ImmutableBytesWritable(
					Bytes.toBytes(Id));

			context.write(HKey, putRow);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "BJYZ-Hbase-odpts-44147.jd.local");
		// conf.set("hbase.client.retries.number", "1");
		conf.set("zookeeper.znode.parent", "/hbase");

		conf.set("column.Family", args[2]);
		conf.set("column.name", args[3]);
		HTable hTable = new HTable(conf, "item_features");

		Job job = Job.getInstance(conf, "hdfsToHbase");

		job.setJarByClass(putNumericVectorBulk.class);
		job.setMapperClass(Map.class);
		// job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		// job.setNumReduceTasks(Integer.parseInt(args[4]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
