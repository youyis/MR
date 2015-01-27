package ajmd

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.hbase.client.{Put, Get, HTable}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.{Writables, Bytes}
import org.apache.hadoop.io.ArrayWritable
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @param hbaseZK
 * @param tableName
 */
class HbasePS(hbaseZK: String, parent: String, tableName: String, familyName: String) {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", hbaseZK)
  conf.set("zookeeper.znode.parent", "/hbase")
  val table = new HTable(conf, tableName)

  val logger = Logger.getRootLogger()

  def pullCate(rowKey: String, familyName: String): Option[Array[(String, Double)]] = {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(familyName))
    val rlist = table.get(get).list()
    if (rlist==null) {
        logger.warn("Item " +rowKey + " does not exist in Cate Feature table!")
        None
    } else {
        val result = new ArrayBuffer[(String, Double)]()
        for(i <- 0 until rlist.size()) {
            val q = Bytes.toString(rlist.get(i).getQualifier())
            val v = Bytes.toString(rlist.get(i).getValue())
            result += ((q+v, 1.0))
        }
        Some(result.toArray)
    }
  }

  def pullNum(rowKey: String, familyName: String): Option[Array[(String, Double)]] = {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(familyName))
    val rlist = table.get(get).list()
    if (rlist==null) {
        logger.warn("Item " +rowKey + " does not exist in Num Feature table!")
        None
    } else {
        val result = new ArrayBuffer[(String, Double)]()
        for(i <- 0 until rlist.size()) {
            val q = Bytes.toString(rlist.get(i).getQualifier())
            val v = Bytes.toDouble(rlist.get(i).getValue())
            result += ((q, v))
        }
        Some(result.toArray)
    }
  }

  def pullNum4VW(rowKey: String, familyName: String): Option[Array[(String, String)]] = {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(familyName))
    val rlist = table.get(get).list()
    if (rlist==null) {
        logger.warn("Item " +rowKey + " does not exist in Num Feature table!")
        None
    } else {
        val result = new ArrayBuffer[(String, String)]()
        for(i <- 0 until rlist.size()) {
            val q = Bytes.toString(rlist.get(i).getQualifier())
            val v = Bytes.toDouble(rlist.get(i).getValue())
            result += ((q, v.toString))
        }
        Some(result.toArray)
    }
  }

  def pullCate4VW(rowKey: String, familyName: String): Option[Array[(String, String)]] = {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(familyName))
    val rlist = table.get(get).list()
    if (rlist==null) {
        logger.warn("Item " +rowKey + " does not exist in Cate Feature table!")
        None
    } else {
        val result = new ArrayBuffer[(String, String)]()
        for(i <- 0 until rlist.size()) {
            val q = Bytes.toString(rlist.get(i).getQualifier())
            val v = Bytes.toString(rlist.get(i).getValue())
            result += ((q, v))
        }
        Some(result.toArray)
    }
  }


  def pull(rowKey: String, columnName: String, weight: DoubleArrayWritable) {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(familyName))
    val result = table.get(get)
    Writables.getWritable(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)), weight)
  }

  def pull(rowKey: String, columnName: String, weight: DoubleWritable) = {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(familyName))
    val result = table.get(get)
    val r = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName))
    if (r != null) {
        Writables.getWritable(r, weight)
    }
  }

  def pull(rowKey: String, columnName: String, weight: ArrayWritable) {
    val get: Get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(familyName))
    val result = table.get(get)
    Writables.getWritable(result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName)), weight)
  }


  def push(rowKey: String, columnNames: Array[String], weights: Array[DoubleArrayWritable]) {
    val put: Put = new Put(Bytes.toBytes(rowKey))
    for (i <- 0 until weights.length) {
      put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnNames(i)), Writables.getBytes(weights(i)))
    }
    table.put(put)
  }


  def push(rowKey: String, columnName: String, weight: DoubleWritable) {
    val put: Put = new Put(Bytes.toBytes(rowKey))
    put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Writables.getBytes(weight))
    table.put(put)
  }

}
