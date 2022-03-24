import java.io.FileWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object apiproject {
  def main(args: Array[String]): Unit = {
    //clientID needed to connect to API, must be hardcoded to function
    val clientID = "ttxorkkbyio5zmvc5ctaueisv45fk1"
    //Call apiConnect to get an access token for further API calls
    val acTkn = apiConnect(clientID)

    //The data we're querying from the API
    val call = "fields id, aggregated_rating, name, genres.name, release_dates.human ; where aggregated_rating > 75 & aggregated_rating != 100; sort aggregated_rating desc; limit 500;"
    val call2 = "fields id, name,release_dates.human, aggregated_rating; where name = *\"Mario\"* & aggregated_rating != null; sort aggregated_rating desc; limit 500;"
    //Call the API and pass it the necessary clientID and access token as well as the specific query we want
    apiCall(clientID, acTkn, call)
    apiCall(clientID, acTkn, call2)


    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE TABLE IF NOT EXISTS user(id INT, username STRING, password STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    var df1=spark.read.option("Multiline",true).json(s"json/${call.substring(7,17)}_out.json")
    df1.show()
    var df2=spark.read.option("Multiline",true).json(s"json/${call2.substring(7,17)}_out.json")
    df2.show()
    spark.close()
  }
  def apiConnect(clientID: String): String ={
    val header = requests.post(s"https://id.twitch.tv/oauth2/token?client_id=${clientID}&client_secret=33yjz3ougtos08iwwr2jk6igvcpifx&grant_type=client_credentials")
    val parsed = ujson.read(header.text)
    println(parsed.render(indent = 4))
    parsed("access_token").str
  }

  //Handles API calls and writes the returned JSON data to a JSON file
  //Stretch goal - add functionality to return more than 500 rows at a time (look at pagination in IGDB docs)
  def apiCall(clientID: String, acTkn: String, call: String): Unit={
    val games = requests.post("https://api.igdb.com/v4/games/",
      data = call,
      headers = Map(
        "Client-ID" -> clientID,
        "Authorization" -> s"Bearer ${acTkn}"
      ))
    val gameParsed = ujson.read(games.text)
    println(gameParsed.render(indent = 4))
    val writer = new FileWriter(s"json/${call.substring(7,17)}_out.json")
    ujson.writeTo(gameParsed, writer, 4)
    writer.flush()
    writer.close()
  }
}
