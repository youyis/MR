package ajmd

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.KafkaInputDStream
import org.apache.spark.streaming.kafka.KafkaReceiver
import kafka.serializer.StringDecoder
//import com.jd.bdp.jdw.kafka.message.JdwDecodeStringMessage
import org.apache.spark.storage.StorageLevel
import com.jd.bdp.jdw.avro.JdwData
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

import spray.json._
import DefaultJsonProtocol._
import org.apache.hadoop.io.DoubleWritable

/*
 class avroJdwDataRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[com.jd.bdp.jdw.avro.JdwData])
    kryo.register(classOf[com.jd.bdp.jdw.kafka.message.JdwDecodeStringMessage])
    kryo.register(classOf[org.apache.avro.Schema])
    kryo.register(classOf[kafka.serializer.StringDecoder])
    kryo.register(classOf[org.apache.spark.storage.StorageLevel])
	}
  }
*/


object popSkuPrice {
 
  
    case class Params(
    hbaseZK: String = null,
    hbaseZkParent: String = null,
    featureTableName: String = null,
    featureFamilyName: String = null,
    featureColumnName: String = null,
    kafkaZK: String = null,
    kafkaGroup: String = null,
    topics: String = null,
    interval: Int = 10,
    numThreads: Int = 5,
    holdout: Int = 100,
    local: Boolean = false)

  
  
  def main(args: Array[String]) {
      
          val sparkConf = new SparkConf().setAppName("popSkuPrice") 
              sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              sparkConf.set("spark.kryo.registrator",  "ajmd.avroJdwDataRegistrator")
          val ssc =  new StreamingContext(sparkConf, Seconds(2))
          val topic = "stream-02-02-10052-sharding-skuprice_pop"
          val topicMap = topic.split(",").map((_, 1)).toMap
          val zookeeper = "ip:port,ip:port/kafka"
                          
          val kafkaParams = Map[String, String]("zookeeper.connect" -> zookeeper, "group.id" -> "jdmp_test")

          val lines =  KafkaUtils.createStream[String, JdwData, StringDecoder, JdwDecodeStringMessage](
                       ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2.toString())
        	
                                
                       
          lines.map(t => {  val cur =  t.parseJson.asJsObject.fields("cur").asJsObject
                            val isSku = cur.fields.contains("skuId")
                            val isSale = cur.fields.contains("salePrice")
                            var skuprice =""
                            if(isSku && isSale){                         
                                val skuid = cur.fields("skuId").toString().replace("\"", "")
                                skuprice = cur.fields("salePrice").toString().replace("\"", "")                                    
                                val hbasePs = new HbasePS("BJYZ-Hbase-odpts-44147", "/hbase",  "item_features", "n")
                                hbasePs.push(skuid, "skuprice", new DoubleWritable(skuprice.toDouble))  
                            }
                            skuprice								
             }).print()       
                             
          ssc.start()
          ssc.awaitTermination()

        }       
    }