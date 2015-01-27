package com.jd.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenBitermCorpus {
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private final HashMap<String, String> termIndex = new HashMap<String, String>();
		private final HashMap<String, String> bitermIndex = new HashMap<String, String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			
			Path termPath = new Path(conf.get("term.index"));
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(termPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] items = line.split("\t");
					termIndex.put(items[0], items[1]);
					line = br.readLine();			
				}
				br.close();
			} catch (Exception e) {
			}
			
			Path bitermPath = new Path(conf.get("biterm.index"));
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(bitermPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] items = line.split("\t");
					StringBuilder str = new StringBuilder("");
					str.append(items[0]);
					str.append(":");
					str.append(items[1]);
					bitermIndex.put(str.toString(), items[2]);
					line = br.readLine();
				}
				br.close();
			} catch (Exception e) {
			}
		}
		
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String[] items = inValue.toString().split("\t");
			int N = items.length;
			if (N < 2) {
				return;
			}
			
			StringBuilder str = new StringBuilder("");
			int count = 0;
			for (int i = 1; i < N; ++i) {
				String[] terms = items[i].split(":");
				String termL = terms[0];
				String termR = terms[1];

				if (termL.compareTo(termR) > 0) {
					String temp = termL;
					termL = termR;
					termR = temp;
				}
				
				StringBuilder biterm = new StringBuilder("");
				biterm.append(termIndex.get(termL));
				biterm.append(":");
				biterm.append(termIndex.get(termR));

				String index  = bitermIndex.get(biterm.toString());
		
				str.append(index);
				str.append("\t");
				
				count ++;
			}
			String lineOut = str.subSequence(0, str.length() - 1).toString();
			context.write(new Text(items[0]),new Text(count + "\t" + lineOut));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("term.index", args[2]);
		conf.set("biterm.index", args[3]);
		conf.set("mapred.child.java.opts", "-Xmx5120m");

		Job job = Job.getInstance(conf);
		job.setJarByClass(GenBitermCorpus.class);
		job.setMapperClass(Map.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
