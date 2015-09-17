import java.io.{FileInputStream, File}
import java.net.URI
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by jiaqige on 4/26/15.
 */
class BfSeqMiner {

}

object BfSeqMiner{
  def main(args: Array[String]) = {

    val propertiesFile = new File(args(0))
    val properties = new Properties()
    properties.loadFromXML(new FileInputStream(propertiesFile))

    val mode = properties.getProperty("mode")
    val inputPath = properties.getProperty("inputPath")
    val outputPath = properties.getProperty("outputPath")
    val splitNum = properties.getProperty("minPartition").toInt

    val support: Double = properties.getProperty("support").toDouble
    val confidence = properties.getProperty("confidence").toDouble
    val minGap = properties.getProperty("minGap").toDouble
    val maxGap = properties.getProperty("maxGap").toDouble

    val jarPath = properties.getProperty("jarPath")
    val jarNames = new Array[String](1)
    jarNames(0) = jarPath

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI(outputPath), hadoopConf)

    try {
      hdfs.delete(new Path(outputPath), true)
    } catch {
      case _: Throwable => {}
    }


    // configure the spark context
    //todo: add jar from property
    val conf = new SparkConf().setAppName("BfSeqMiner").setMaster(mode).setJars(jarNames)

    val sc = new SparkContext(conf)

    //    if(mode != "local")
    //      sc.addJar("seqminer.jar")


    //read data to RDD and transform it to the vertical format

    val seqMiner = new SeqMiner(support, confidence, minGap, maxGap)

    val data = seqMiner.readSeqFile(sc, inputPath, splitNum)

    //mine sequential patterns with quality measurements
    val frequentPatternsRDD = seqMiner.seqMine(sc, data)

    //save output to hdfs

    if (frequentPatternsRDD != null) {
      frequentPatternsRDD.repartition(1).saveAsTextFile(outputPath)

      //generate rules
      //val rules = seqMiner.buildRules(frequentPatternsRDD)
      //println(rules)
    }
  }
}