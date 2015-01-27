package ajmd

import com.jd.bdp.jdw.avro.JdwData
import kafka.utils.VerifiableProperties
import kafka.serializer.Decoder
import com.jd.bdp.jdw.util.AvroCoderUtils
import scala.collection.JavaConversions.asScalaSet

/**
 * Created by haisen on 15-1-22.
 */
class JdwDecodeStringMessage  (props: VerifiableProperties = null) extends Decoder[JdwData] {

  def fromBytes(bytes: Array[Byte]): JdwData = {
    val jdwData :JdwData  = AvroCoderUtils.decode[JdwData](bytes, JdwData.getClassSchema)
    val sourceMap = jdwData.getSrc
    val stringSourceMap = new java.util.HashMap[CharSequence, CharSequence]
    if (sourceMap != null && sourceMap.size() > 0) {
      for (entry <- sourceMap.entrySet()) {
        val key = if(entry.getKey == null) null else entry.getKey.toString
        val value = if(entry.getValue == null) null else entry.getValue.toString
        stringSourceMap.put(key,value)
      }
    }
    val currentMap = jdwData.getCur
    val stringCurrentMap = new java.util.HashMap[CharSequence, CharSequence]
    if (currentMap != null && currentMap.size() > 0) {
      for (entry <- currentMap.entrySet()) {
        val key = if(entry.getKey == null) null else entry.getKey.toString
        val value = if(entry.getValue == null) null else entry.getValue.toString
        stringCurrentMap.put(key,value)
      }
    }
    val cusMap = jdwData.getCus
    val stringCusMap = new java.util.HashMap[CharSequence, CharSequence]
    if (cusMap != null && cusMap.size() > 0) {
      for (entry <- cusMap.entrySet()) {
        val key = if(entry.getKey == null) null else entry.getKey.toString
        val value = if(entry.getValue == null) null else entry.getValue.toString
        stringCusMap.put(key,value)
      }
    }
    jdwData.setSrc(stringSourceMap)
    jdwData.setCur(stringCurrentMap)
    jdwData.setCus(stringCusMap)
    jdwData.setDb(if(jdwData.getDb== null) null else jdwData.getDb)
    jdwData.setDdl(if(jdwData.getDdl == null) null else jdwData.getDdl)
    jdwData.setErr(if(jdwData.getErr == null) null else jdwData.getErr)
    jdwData.setOpt(if(jdwData.getOpt== null) null else jdwData.getOpt)
    jdwData.setSch(if(jdwData.getSch == null) null else jdwData.getSch)
    jdwData.setTab(if(jdwData.getTab == null) null else jdwData.getTab)
    jdwData
  }
}