package de.peedeex21.twittertraffic

import de.peedeex21.twittertraffic.TwitterTrafficJob.AccidentTweet
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap
import org.apache.flink.streaming.connectors.twitter.TwitterFilterSource
import org.apache.flink.util.Collector
import org.apache.sling.commons.json.JSONException
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.libs.Json

object TwitterTrafficJob {

  var loginProperties: Option[String] = None
  var outputPath: Option[String] = None

  /**
   * Wrapper for accident POIs.
   *
   * @param id tweet id
   * @param createdAt date of creation
   * @param text tweet text
   * @param lon appended longitude
   * @param lat appended latitude
   */
  case class AccidentTweet(id: Long, createdAt: DateTime, text: String, lang: String, lon: Double, lat: Double) {
    override def toString = {
      this.id + "," + this.createdAt + "," + StringEscapeUtils.escapeCsv(this.text) + "," +
        this.lang + "," + this.lon + "," + this.lat
    }
  }

  def main(args: Array[String]) {

    // parse the program arguments
    if(!parseArguments(args)) {
      return
    }

    // set up the streaming execution environment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // set up the data source, filter for the term 'accident' or 'traffic'
    val twitterSource = new TwitterFilterSource(loginProperties.get)
    twitterSource.trackTerm("accident")
    twitterSource.trackTerm("traffic")
    twitterSource.getLocations

    // retrieve, filter and parse tweets
    val accidentPOIs = streamEnv.addSource(twitterSource)
      .flatMap(new AccidentTweetParser())

    // emit the result
    if(outputPath.isDefined) {
      accidentPOIs.writeAsText(outputPath.get)
    } else {
      accidentPOIs.print()
    }

    // execute program
    streamEnv.execute("Twitter Traffic")
  }

  /**
   * Parser function for program arguments.
   *
   * @param args path/to/login.properties, [path/to/output]
   * @return true on success otherwise false
   */
  def parseArguments(args: Array[String]): Boolean = {
    if(args.length == 1) {
      this.loginProperties = Some(args(0))
      true
    } else if(args.length == 2) {
      this.loginProperties = Some(args(0))
      this.outputPath = Some(args(1) + "/" + new DateTime().getMillis)
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

class AccidentTweetParser extends JSONParseFlatMap[String, AccidentTweet] {

  // e.g. Thu Oct 15 17:10:31 +0000 2015
  val inputDatePattern = "EEE MMM dd HH:mm:ss Z yyyy"

  override def flatMap(in: String, out: Collector[AccidentTweet]): Unit = {

    try {
      if (getString(in, "coordinates").equals("null")) {
        // no geo location given
        return
      }

      if(getBoolean(in, "retweeted")) {
        // no need for duplicates
        return
      }

      val targetDateFormat = DateTimeFormat.forPattern(inputDatePattern).withOffsetParsed()

      val id = getLong(in, "id")
      val createdAt = targetDateFormat.parseDateTime(getString(in, "created_at"))
      val text = getString(in, "text")
      val lang = getString(in, "lang")
      val coordinates = Json.parse(getString(in, "coordinates")).get("coordinates")

      out.collect(new AccidentTweet(id, createdAt, text, lang,
        coordinates.get(0).asDouble, coordinates.get(1).asDouble))
    } catch {
      case e: JSONException => //ignore malformed JSON
    }
  }

}
