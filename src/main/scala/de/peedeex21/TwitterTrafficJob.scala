package de.peedeex21

import java.util.Date

import de.peedeex21.TwitterTrafficJob.AccidentTweet
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap
import org.apache.flink.streaming.connectors.twitter.TwitterFilterSource
import org.apache.flink.util.Collector
import play.libs.Json

object TwitterTrafficJob {

  var loginProperties: Option[String] = None
  var outputPath: Option[String] = None

  case class AccidentTweet(id: Long, createdAt: Date, text: String, lon: Double, lat: Double) {
    override def toString = {
      this.id + "," + this.createdAt.getTime + ",\"" +  this.text + "\"," + this.lon + "," + this.lat
    }
  }

  def main(args: Array[String]) {

    // parse the program arguments
    if(!parseArguments(args)) {
      return
    }

    // set up the streaming execution environment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // set up the data source, filter for the term 'unfall'
    val twitterSource = new TwitterFilterSource(loginProperties.get)
    twitterSource.trackTerm("accident")
    twitterSource.trackTerm("traffic")
    twitterSource.getLocations
    //twitterSource.filterLanguage("de")

    val accidentPOIs = streamEnv.addSource(twitterSource)
      .flatMap(new AccidentTweetMapper())

    // emit the result
    if(outputPath.isDefined) {
      accidentPOIs.writeAsCsv(outputPath.get)
    } else {
      accidentPOIs.print()
    }

    // execute program
    streamEnv.execute("Twitter Traffic")
  }

  def parseArguments(args: Array[String]): Boolean = {
    if(args.length == 1) {
      this.loginProperties = Some(args(0))
      true
    } else if(args.length == 2) {
      this.loginProperties = Some(args(0))
      this.outputPath = Some(args(1))
      true
    } else {
      println(
        """
          |Wrong number of arguments given.
          | Usage: TwitterTrafficJob <path/to/login.properties> [<path/to/output>]
        """.stripMargin)
      false
    }
  }

}

class AccidentTweetMapper extends JSONParseFlatMap[String, AccidentTweet] {

  // e.g. Thu Oct 15 17:10:31 +0000 2015
  val dateFormat = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")

  override def flatMap(in: String, out: Collector[AccidentTweet]): Unit = {

    if (getString(in, "coordinates").equals("null")) {
      // no geo location given
      return
    }

    val id = getLong(in, "id")
    val createdAt = dateFormat.parse(getString(in, "created_at"))
    val text = getString(in, "text")
    val coordinates = Json.parse(getString(in, "coordinates")).get("coordinates")

    out.collect(new AccidentTweet(id, createdAt, text,
      coordinates.get(0).asDouble, coordinates.get(1).asDouble))

  }
}
