package com.jd.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SampleComment {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

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
					skuSet.add(line.trim());
					line = br.readLine();
				}
			} catch (Exception e) {
			}

		}

		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\t");
			if (items.length < 2) {
				return;
			}

			String skuId = items[0];
			if (skuId.equals("NULL") || skuId.equals("null"))
				return;
			if (!skuSet.contains(skuId))
				return;
			
			StringBuilder str = new StringBuilder("");
			for(int i =1;i < items.length;i++){
				str.append(items[i]);
				str.append(" ");
			}
            String lineOut = str.subSequence(0, (str.length() - 1)).toString();
			context.write(new Text(line), new Text(lineOut));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("sku.set", args[2]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(SampleComment.class);
		job.setMapperClass(Map.class);
//		job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);

//		job.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
