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

public class CategorySku {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private final HashSet<String> categorySet = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path skuPath = new Path(conf.get("category.set"));

			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(skuPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					categorySet.add(line.trim());
					line = br.readLine();
				}
			} catch (Exception e) {
			}

		}

		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\t");
			if (items.length < 8) {
				return;
			}
            String categoryId =items[4];
			String skuId = items[6];
			if (skuId.equals("NULL") || skuId.equals("null"))
				return;
			if (!categorySet.contains(categoryId))
				return;
			
			context.write(new Text(categoryId), new Text(skuId));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("category.set", args[2]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(CategorySku.class);
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
