package com.jd.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;

public class createHbase {

	// ���� HBase ���ݱ�
	public static void createHBaseTable(String tableName) throws IOException {
		// ����������
		HTableDescriptor htd = new HTableDescriptor(tableName);
		// ������������
		
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
		
		// ���� HBase
		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", "BJYZ-Hbase-odpts-44147.jd.local");

		conf.set("zookeeper.znode.parent", "/hbase");

		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("�����ݱ��Ѿ����ڣ��������´�����");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}

		System.out.println("������" + tableName);
		hAdmin.createTable(htd);
		System.out.println("�������");
		hAdmin.close();
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String tableName = "item_features";
	//	String tableName = "item_item_toplist";
		createHbase.createHBaseTable(tableName);

	}

}
