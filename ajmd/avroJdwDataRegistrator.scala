package ajmd

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

 class avroJdwDataRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[com.jd.bdp.jdw.avro.JdwData])
    kryo.register(classOf[com.jd.bdp.jdw.kafka.message.JdwDecodeStringMessage])
    kryo.register(classOf[org.apache.avro.Schema])
    kryo.register(classOf[kafka.serializer.StringDecoder])
    kryo.register(classOf[org.apache.spark.storage.StorageLevel])
	}
  }