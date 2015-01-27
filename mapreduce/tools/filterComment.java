package com.jd.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mockito.internal.matchers.Find;

public class filterComment {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private final HashSet<String> stopwords = new HashSet<String>();
		private final HashSet<String> posSet = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path stopwordPath = new Path(conf.get("stopword.file"));
			Path posPath = new Path(conf.get("wordpos.file"));

			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(stopwordPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					stopwords.add(line.trim());
					line = br.readLine();
				}
			} catch (Exception e) {
			}

			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(posPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					posSet.add(line.trim());
					line = br.readLine();
				}
			} catch (Exception e) {
			}

		}

		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\\s+");
			String skuId = items[0];
			Pattern pattern = Pattern.compile("[a-zA-Z0-9.]{10,}");

			StringBuilder str = new StringBuilder("");
			for (int i = 3; i < items.length; i++) {
				String[] tempItem = items[i].split("/");
				if (tempItem.length < 2)
					continue;
				String word = tempItem[0];
				String pos = tempItem[1];
				word = word.replace(":", "");
				if (word.length() <= 1)
					continue;
				if (!stopwords.contains(word)) {
					Matcher matcher = pattern.matcher(word);
					boolean b = matcher.matches();
					if (b)
						continue;
					for (String apos : posSet) {
						if (pos.indexOf(apos, 0) == 0) {
							str.append(word);
							str.append("\t");
							break;
						}
					}
				}
			}

			if (str.toString().equals(""))
				return;

			String lineOut = str.subSequence(0, str.length() - 1).toString();
			context.write(new Text(skuId), new Text(lineOut));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("stopword.file", args[2]);
		conf.set("wordpos.file", args[3]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(filterComment.class);
		job.setMapperClass(Map.class);
		// job.setReducerClass(Reduce.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
