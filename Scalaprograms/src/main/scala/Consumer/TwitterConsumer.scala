package Consumer

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Dharmam on 4/15/2017.
  */
object TwitterConsumer {

  def main(args: Array[String]): Unit = {

    Util.LoggerUtil.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[*]").setSparkHome("/usr/local/spark").setAppName("Kafka Consumer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))


    val (zkQuorum, group, topics, numThreads) = ("localhost", "localhost", "test", "20")
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)


    lines.foreachRDD({
      rdd =>
        rdd.foreach(
          l => {
           // val numbers = Map(l._1.toString -> Util.Analyzer.mainSentiment(l._2).toString)
            println(l._1.toString())
            println(Util.Analyzer.mainSentiment(l._2))
          }
        )
    })

    ssc.start()
    ssc.awaitTermination()
  }

}