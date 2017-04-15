package Producer

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Dharmam on 4/15/2017.
  */
object TwitterProducer {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    Util.LoggerUtil.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)


    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka Producer")


    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters) //Dstream

    val englishTweets = stream.filter(_.getLang == "en")


    val userTweets = englishTweets.filter(_.getUser.getLocation != null)


    userTweets.foreachRDD({
      rdd =>
        rdd.foreach({
          tweet => {
            val kafkaOpTopic = "test"
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              "org.apache.kafka.common.serialization.StringSerializer")
            val producer = new KafkaProducer[String, String](props)
            val data = tweet.getText
            tweet.getHashtagEntities.foreach(
              tag => {
                val key = tag.getText
                val message = new ProducerRecord[String, String](kafkaOpTopic, key.toString, data.toString)
                println("key => " + key.toString + " and value => " + data.toString)
                producer.send(message)
              })
            producer.close()

          }
        })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
