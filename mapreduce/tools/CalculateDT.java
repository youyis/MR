package com.jd.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

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

import com.jd.common.*;

public class CalculateDT {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private  ArrayList<ArrayList<Double>> Pzw = new ArrayList<ArrayList<Double>>();

		private final ArrayList<Double> theta = new ArrayList<Double>();

	//	private final ArrayList<IntPair> bitermIndexMap = new ArrayList<IntPair>();

		private final ArrayList<ArrayList<Double>> Pwz = new ArrayList<ArrayList<Double>>();

		private int topicNum = 0;



		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path phiPath = new Path(conf.get("phi.file"));
			Path thetaPath = new Path(conf.get("theta.file"));
			Path bitermIndexPath = new Path(conf.get("bitermIndex.file"));

			// load Pzw
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(phiPath)));
				String line;
				line = br.readLine();
				ArrayList<Double> linePhi;
				while (line != null) {
					 linePhi = new ArrayList<Double>();
					String[] items = line.trim().split(" ");
					for (String item : items) {
						linePhi.add(Double.parseDouble(item));
					}
					Pzw.add(linePhi);
					line = br.readLine();
				}
			} catch (Exception e) {
			}

			topicNum = Pzw.size();


			// load theta
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(thetaPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] items = line.trim().split(" ");
					for (String item : items) {
						theta.add(Double.parseDouble(item));
					}

					line = br.readLine();
				}
			} catch (Exception e) {
			}

			
			// load bitermIndexMap and compute Pwz
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(bitermIndexPath)));
				String line;
				line = br.readLine();
				ArrayList<Double> bitermLine;
				while (line != null) {
					String[] items = line.trim().split("\t");
					if (items.length < 3)
						continue;
					bitermLine = new ArrayList<Double>();
					Double sum = 0.0;
					int first = Integer.parseInt(items[0]);
					int second = Integer.parseInt(items[1]);
					for (int j = 0; j < topicNum; j++) {
						double bitermPwz = theta.get(j) * Pzw.get(j).get(first)
								* Pzw.get(j).get(second);
						sum += bitermPwz;
						bitermLine.add(bitermPwz);
					}
					
					for (int k = 0; k < topicNum; k++) {
						bitermLine.set(k, bitermLine.get(k) / sum);
					}
					Pwz.add(bitermLine);
					line = br.readLine();
				}
			} catch (Exception e) {
			}
			
			
			Pzw = null;
			
			/*
			// load bitermIndexMap
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(bitermIndexPath)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] items = line.trim().split("\t");
					if (items.length < 3)
						continue;
					IntPair biterm = new IntPair();
					biterm.set(Integer.parseInt(items[0]),
							Integer.parseInt(items[1]));
					bitermIndexMap.add(biterm);
					line = br.readLine();
				}
			} catch (Exception e) {
			}

			// calculate Pzw
			int first = -1;
			int second = -1;
			int bitermLen = bitermIndexMap.size();
			for (int i = 0; i < bitermLen; i++) {
				ArrayList<Double> bitermLine = new ArrayList<Double>();
				Double sum = 0.0;
				for (int j = 0; j < topicNum; j++) {
					first = bitermIndexMap.get(i).getFirst();
					second = bitermIndexMap.get(i).getSecond();
					double bitermPwz = theta.get(j) * Pzw.get(j).get(first)
							* Pzw.get(j).get(second);
					sum += bitermPwz;
					bitermLine.add(bitermPwz);
				}

				for (int k = 0; k < topicNum; k++) {
					bitermLine.set(k, bitermLine.get(k) / sum);
				}

				Pwz.add(bitermLine);
			}*/

		}

		public void map(LongWritable inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			String line = inValue.toString();
			String[] items = line.split("\t");
			int itemLen = items.length;
			if (itemLen < 2) {
				return;
			}

			String docId = items[0];
			Double[] docTopic = new Double[topicNum];
			StringBuilder str = new StringBuilder("");
			for (int i = 0; i < topicNum; i++) {
				Double sum = 0.0;
				for (int j = 1; j < itemLen; j++) {
					docTopic[i] = Pwz.get(Integer.parseInt(items[j])).get(i);
					sum += docTopic[i];
				}
				docTopic[i] /= sum;
				str.append(docTopic[i].toString());
				str.append(" ");
			}

			String lineOut = str.subSequence(0, str.length() - 1).toString();

			context.write(new Text(docId), new Text(lineOut));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("phi.file", args[2]);
		conf.set("theta.file", args[3]);
		conf.set("bitermIndex.file", args[4]);
		conf.set("mapred.child.java.opts", "-Xmx7000m");
		

		Job job = Job.getInstance(conf);
		job.setJarByClass(CalculateDT.class);
		job.setMapperClass(Map.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// job.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
