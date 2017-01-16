import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * Created by hdfs on 2017/1/15.
  */
object SparkKafka {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf
    conf.setSparkHome("/opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark")
    conf.setMaster("spark://cdh001:7077")
    conf.setAppName("KafkaSpark")
    conf.set("SPARK_EXECUTOR_MEMORY", "1g")

    var sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(2))
    val zkQuorum = "cdh001:2181,cdh002:2181,cdh003:2181,cdh004:2181"
    val group = "test-consumer-group"
    val topics = "renluo"
    val numThreads = 1
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    val lines = lineMap.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val pair = words.map(x => (x,1))
    val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(2),2)
    wordCounts.print
    ssc.checkpoint("/user/spark/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
