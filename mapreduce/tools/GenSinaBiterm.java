package com.jd.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenSinaBiterm {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		private final HashSet<String> stopwords = new HashSet<String>();
		private final HashSet<String> posSet = new HashSet<String>();

		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path stopwordPath = new Path(conf.get("stopword.file"));           
			
			//load stopwords
			try {			
				FileSystem fs = FileSystem.get(context.getConfiguration());
				if(fs.exists(stopwordPath)) System.out.println("exist..............");
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(stopwordPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					stopwords.add(line.trim());
					System.out.println(line);
					line = br.readLine();
				}
				br.close();
			} catch (Exception e) {
			}

			
			//load word pos
			Path posPath = new Path(conf.get("wordpos.file"));
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(posPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					posSet.add(line.trim());
					line = br.readLine();
				}
				br.close();
			} catch (Exception e) {
			}
		}

		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			
			
			String[] tokens = inValue.toString().split("\\s+");
			String sku = tokens[0];
			String[] skuItem = sku.split("/");
			String skuId = skuItem[0];
			
			Pattern pattern = Pattern.compile("[a-zA-Z]+[0-9]+\\w{0,}|%?[0-9]+\\w{0,}|\\w+-\\w+|^[&#].*");
			
			int N = tokens.length;
			if (N <= 2) {
				return;
			}
            
			
			List<String> titleTermList = new ArrayList<String>();
			for (int i = 1; i < N; i++) {
				String[] tempItem = tokens[i].split("/");
				if (tempItem.length < 2)
					continue;
				String word = tempItem[0];
				String type = tempItem[1];
				word = word.replace(":", "");
				if (word.length() <= 1)
					continue;
				if (!stopwords.contains(word)) {
					Matcher matcher = pattern.matcher(word);
					boolean b = matcher.matches();
					if (b)
						continue;
					for (String apos : posSet) {
						if (type.indexOf(apos, 0) == 0) {
							titleTermList.add(word);
							break;
						}
					}
				}
			}
			
			N = titleTermList.size();
			if (N <= 1) {
				return;
			}
			StringBuilder str = new StringBuilder("");
			// take 3 words to construct biterm 
			for (int i = 0; i < N -1; i ++) {
				String stri = titleTermList.get(i);
				int count = 0; 
				for (int j = i + 1; j < N; j ++) {
					
					String strj = titleTermList.get(j);
					if (stri.compareTo(strj) != 0) { 
						str.append(stri);
						str.append(":");
						str.append(strj);
						str.append("\t");
						count ++;
					}
					if (count >2) break;
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
		job.setJarByClass(GenSinaBiterm.class);
		job.setMapperClass(Map.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is Text, IntWritable
	//	job.setOutputKeyClass(Text.class);
	//	job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}
}
