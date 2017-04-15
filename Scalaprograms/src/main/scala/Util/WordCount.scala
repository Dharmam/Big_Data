package Util

/**
  * Created by Dharmam on 4/15/2017.
  */
import org.apache.spark._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val (zkQuorum, group, topics, numThreads) = ("localhost", "localhost", "test", "20")

    LoggerUtil.setStreamingLogLevels()

    val sparkConf = new SparkConf().setMaster("local[*]").setSparkHome("/usr/local/spark").setAppName("MyWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    //val topics = "test"
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x.toLowerCase, 1L))
      .reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
