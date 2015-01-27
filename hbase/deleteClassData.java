package com.jd.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Delete;

public class deleteClassData {
	public static class Map extends TableMapper<Text, Text> {
		private String columnCluster = "";
		private String columnName = "";

		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			columnCluster = conf.get("column.cluster");
			columnName = conf.get("column.name");

		}

		@Override
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			HTable htbl = new HTable(conf, "item_features");
			List<Delete> lists = new ArrayList<Delete>();
			for (KeyValue kv : value.raw()) {
				Delete dlt = new Delete(kv.getRow());
				dlt.deleteColumn(columnCluster.getBytes(),
						columnName.getBytes());
				lists.add(dlt);
		/*		System.out.println("delete-- gv:" + Bytes.toString(kv.getRow())
								+ ",family:"
								+ Bytes.toString(columnCluster.getBytes())
								+ ",qualifier:"
								+ Bytes.toString(columnName.getBytes()));*/
			}

			htbl.delete(lists);
			htbl.flushCommits();
			htbl.close();
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "BJYZ-Hbase-odpts-44147.jd.local");
		conf.set("hbase.client.retries.number", "1");
		conf.set("zookeeper.znode.parent", "/hbase");

		conf.set("column.cluster", args[0]);
		conf.set("column.name", args[1]);
		Job job = Job.getInstance(conf, "hbaseToDelete");

		job.setJarByClass(deleteClassData.class);
//		job.setMapperClass(Map.class);
		// job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Scan scan = new Scan();
		scan.setCaching(1500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("item_features", scan, Map.class,
				null, null, job);

		job.setOutputFormatClass(NullOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
