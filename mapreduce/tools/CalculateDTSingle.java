package com.jd.tools;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;

public class CalculateDTSingle {

	ArrayList<ArrayList<Double>> Pzw = new ArrayList<ArrayList<Double>>();
	ArrayList<Double> theta = new ArrayList<Double>();
	ArrayList<ArrayList<Double>> Pwz = new ArrayList<ArrayList<Double>>();
	int topicNum = 0;
	private FileWriter fw;

	// load Pzw
	public void loadPzw(String fileName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String line = br.readLine();
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
			br.close();
			topicNum = Pzw.size();
		} catch (Exception e) {
		}
	}

	// load theta
	public void loadTheta(String fileName) {

		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String line = br.readLine();
			while (line != null) {
				String[] items = line.trim().split(" ");
				for (String item : items) {
					theta.add(Double.parseDouble(item));
				}

				line = br.readLine();
			}
			br.close();
		} catch (Exception e) {
		}
	}

	// load bitermIndexMap and compute Pwz
	public void computePwz(String fileName) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));
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
			br.close();
		} catch (Exception e) {
		}

	}

	public void computeDT(String corpusFileName, String outFileName)
			throws IOException {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(
					corpusFileName));
			fw = new FileWriter(outFileName);
			String line = br.readLine();
			int n = 0;
			while (line != null) {
				String[] items = line.split("\t");
				int itemLen = items.length;
				if (itemLen < 2) {
					return;
				}
				n++;
				if (n % 1000 == 0)
					System.out.println("has deal with " + n);
				String docId = items[0];
				Double[] docTopic = new Double[topicNum];
				StringBuilder str = new StringBuilder("");
				for (int i = 0; i < topicNum; i++) {
					Double sum = 0.0;
					for (int j = 1; j < itemLen; j++) {
						docTopic[i] = Pwz.get(Integer.parseInt(items[j]))
								.get(i);
						sum += docTopic[i];
					}
					docTopic[i] /= sum;
					str.append(docTopic[i].toString());
					str.append(" ");
				}
				String lineOut = str.subSequence(0, str.length() - 1)
						.toString();
				fw.write(docId + "\t" + lineOut + "\n");
				line = br.readLine();
			}
			br.close();
			fw.flush();
			fw.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {
		System.out
				.println("please input phiFile thetaFile bitermIndexFile corpusFile outFile");
		System.out.println(args.length);
		String phiFile = args[0];
		String thetaFile = args[1];
		String bitermIndexFile = args[2];
		String corpusFile = args[3];
		String outFile = args[4];
		CalculateDTSingle cal = new CalculateDTSingle();
		cal.loadPzw(phiFile);
		cal.loadTheta(thetaFile);
		cal.computePwz(bitermIndexFile);
		cal.computeDT(corpusFile, outFile);

	}

}
