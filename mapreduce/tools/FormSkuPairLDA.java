package com.jd.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FormSkuPairLDA {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String[] items = inValue.toString().split("\t");

			String skuL = items[0];
			String skuR = items[1];
			String count = items[2];
			String valueR = skuR + ":" + count;
			String valueL = skuL + ":" + count;

			context.write(new Text(skuL), new Text(valueR));
			context.write(new Text(skuR), new Text(valueL));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// private MultipleOutputs mos;
		private int DocLength = 10;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			// mos = new MultipleOutputs(context);
			Configuration conf = context.getConfiguration();
			DocLength = conf.getInt("Doc.Length", 10);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder str = new StringBuilder("");
			int count = 0;
			for (Text value : values) {
				str.append(value);
				str.append("\t");
				count += 1;
			}
			if (count < DocLength)
				return;

			Double rand = Math.random();
			if (rand < 0.8)
				return;

			String line = str.subSequence(0, str.length() - 1).toString();
			context.write(key, new Text(count + "\t" + line));

			// mos.write(key, NullWritable.get(), "sku/skuid");
		}
		/*
		 * @Override protected void cleanup(Context context) throws IOException,
		 * InterruptedException { super.cleanup(context); mos.close(); }
		 */
	}

	

    //Convert Unix timestamp to normal date style  
    public String TimeStamp2Date(String timestampString){  
      Long timestamp = Long.parseLong(timestampString)*1000;  
      String date = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new java.util.Date(timestamp));  
      return date;  
    }  

	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("Doc.Length", args[2]);
		Job job = Job.getInstance(conf);
		job.setJarByClass(FormSkuPairLDA.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
