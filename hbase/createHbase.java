package com.jd.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;

public class createHbase {

	// 创建 HBase 数据表
	public static void createHBaseTable(String tableName) throws IOException {
		// 创建表描述
		HTableDescriptor htd = new HTableDescriptor(tableName);
		// 创建列族描述
		
		HColumnDescriptor col1 = new HColumnDescriptor("c");
		col1.setCompressionType(Algorithm.SNAPPY);
		htd.addFamily(col1);		
		HColumnDescriptor col2 = new HColumnDescriptor("n");
		col2.setCompressionType(Algorithm.SNAPPY);
		htd.addFamily(col2);
		
        
		/*
		HColumnDescriptor col1 = new HColumnDescriptor("toplist");
		col1.setCompressionType(Algorithm.SNAPPY);
		htd.addFamily(col1);
		*/
		
		// 配置 HBase
		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", "BJYZ-Hbase-odpts-44147.jd.local");

		conf.set("zookeeper.znode.parent", "/hbase");

		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("该数据表已经存在，正在重新创建。");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}

		System.out.println("创建表：" + tableName);
		hAdmin.createTable(htd);
		System.out.println("创建完成");
		hAdmin.close();
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String tableName = "item_features";
	//	String tableName = "item_item_toplist";
		createHbase.createHBaseTable(tableName);

	}

}
