import java.io.FileWriter
import org.apache.spark.sql.SparkSession

import scala.io.StdIn._
import java.time.LocalDate

object apiproject {
  def main(args: Array[String]): Unit = {

    //Sets up the spark environment
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("IGDB query")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("Created Spark session\n")
    spark.sparkContext.setLogLevel("ERROR")

    //spark.sql("DROP TABLE IF EXISTS user")
    spark.sql("CREATE TABLE IF NOT EXISTS user(id INT, username STRING, password STRING, access STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")
    //spark.sql("INSERT INTO user VALUES ('1', 'admin', 'password', 'admin')")

    if (accountCheck(spark)) {
      if(login(spark)) {
        //clientID needed to connect to API, must be hardcoded to function
        val clientID = "ttxorkkbyio5zmvc5ctaueisv45fk1"
        //Call apiConnect to get an access token for further API calls
        val acTkn = apiConnect(clientID)

        //Call yearInput to get the user's chosen year
        val callYear = yearInput()

        //Call the API and pass it the necessary clientID and access token as well as the specific query we want
        apiCall(clientID, acTkn, callYear)

        println("\nLoading JSON data into dataframes...\n")
        val df1 = spark.read.option("Multiline", true).json(s"json/${callYear}_out.json")
        //df1.createGlobalTempView("games")

        spark.sql("DROP TABLE IF EXISTS bucketed")
        df1.write.format("parquet").partitionBy("category").bucketBy(5, "platforms").sortBy("name").saveAsTable("bucketed")
        //spark.sql("SHOW PARTITIONS bucketed").show()

        sparkQueries(callYear, spark)
      }
    }

    spark.close()
  }

  //Query the bucketed table with a selection of queries
  def sparkQueries(callYear: Int, spark: SparkSession):Unit={
    println(s"All the game data from $callYear in the bucketed and partitioned table:")
    spark.sql("SELECT * FROM bucketed").show()

    println(s"The average rating of games released in $callYear for each group of platforms:")
    spark.sql("SELECT platforms.name, AVG(aggregated_rating) AS avg_rating FROM bucketed GROUP BY platforms.name ORDER BY avg_rating desc").show()

    println(s"Mario games released in $callYear")
    spark.sql("SELECT aggregated_rating, genres.name, name, platforms.name, release_dates FROM bucketed WHERE name LIKE '%Mario%' ORDER BY name ").show()


    println(s"The number of games in each category released in $callYear")
    spark.sql("SELECT COUNT(category) as category_count, category FROM bucketed GROUP BY category ORDER BY category_count desc").show()

    println(s"Every shooter released in $callYear sorted by rating")
    spark.sql("SELECT name, aggregated_rating, genres.name FROM bucketed WHERE array_contains(genres.name, 'Shooter') ORDER BY aggregated_rating desc").show()

    println(s"The number of games released per genre group in $callYear")
    spark.sql("SELECT COUNT(genres) as genre_count, genres.name FROM bucketed GROUP BY genres ORDER BY genre_count desc").show()

    println(s"The top 10 most reviewed games released in $callYear ")
    spark.sql("SELECT aggregated_rating, genres.name, name, platforms.name, rating_count FROM bucketed ORDER BY rating_count desc LIMIT 10").show()

    println(s"The number of games containing a number in the title in $callYear ")
    spark.sql("SELECT COUNT(name) AS title_count FROM bucketed WHERE name LIKE '%\\d%'").show()
  }

  //Connects to the API using clientID and returns an accTkn
  def apiConnect(clientID: String): String ={
    println("\nConnecting to API...")
    val header = requests.post(s"https://id.twitch.tv/oauth2/token?client_id=$clientID&client_secret=33yjz3ougtos08iwwr2jk6igvcpifx&grant_type=client_credentials")
    val parsed = ujson.read(header.text)
    //println(parsed.render(indent = 4))
    println("API access approved.")
    parsed("access_token").str
  }

  //Handles API calls and writes the returned JSON data to a JSON file
  //Stretch goal - add functionality to return more than 500 rows at a time (look at pagination in IGDB docs)
  def apiCall(clientID: String, acTkn: String, callYear: Int): Unit={
    //The data we're querying from the API
    val call = s"fields id, aggregated_rating, name, genres.name, release_dates.human, platforms.name, category, rating_count; where aggregated_rating > 60 & aggregated_rating != 100 & release_dates.y = $callYear; sort aggregated_rating desc; limit 500;"
    println("\nCalling API...")
    val games = requests.post("https://api.igdb.com/v4/games/",
      data = call,
      headers = Map(
        "Client-ID" -> clientID,
        "Authorization" -> s"Bearer $acTkn"
      ))
    val gameParsed = ujson.read(games.text)
    //println(gameParsed.render(indent = 4))
    val writer = new FileWriter(s"json/${callYear}_out.json")
    ujson.writeTo(gameParsed, writer, 4)
    writer.flush()
    writer.close()
    println("API call done. Writing results to JSON file.")
  }

  //Checks if user has an account, and if they don't allow them to create one.
  //Return true if they have an account, false if they don't
  def accountCheck(spark: SparkSession): Boolean={
    var accExist = 2
    var exitCheck = false

    while(accExist != 0 & accExist !=1 ){
      var scanAcc = readLine("Do you have an account? [Y]es/[N]o ")
      scanAcc match{
        //If user has an account, exit while loop with code 1
        case y if y matches "(?i)Y" => accExist = 1
        //If user does not have an account, check if they want to create one
        case n if n matches "(?i)N" =>
          while(!exitCheck){
            scanAcc = readLine("Would you like to create an account? [Y]es/[N]o ")
            scanAcc match{
              case y if y matches "(?i)Y" =>
                val scanUser = readLine("Please enter your desired username: ")
                val scanPass = readLine("Please enter your desired password: ")
                val credentials = spark.sql(s"SELECT id FROM user WHERE username = '$scanUser'")
                try{
                  //Check the user database if your desired username already exists, if it does then enter another
                  credentials.head().getInt(0)
                  println("Username already exists. Please attempt account creation again.")
                }
                catch{
                  //Check the user database if your desired username already exists, if it doesn't insert your user and pass into db
                  case _: NoSuchElementException => println("Creating account...")
                    //Stretch goal - encrypt password
                    val userNum = spark.sql("SELECT MAX(user.id) FROM user").head().getInt(0)
                    spark.sql(s"INSERT INTO user VALUES (${userNum + 1}, '$scanUser', '$scanPass', 'Basic')")
                    accExist = 1
                    exitCheck = true
                }
              case n if n matches "(?i)N" =>
                println("Thank you. Now exiting program.")
                accExist = 0
                exitCheck = true
              case _ => println("Invalid input. Please input [Y]es/[N]o ")
            }
          }

        case _ => println("Invalid input. Please input [Y]es/[N]o ")
      }
    }
    if(accExist == 1) true else false
  }

  //Logs user into application if they have valid credentials
  //Returns true if user logs in successfully, false if they don't
  def login(spark: SparkSession):Boolean={
    var validCred = 2
    var exitCheck = false

    while(validCred != 0 & validCred != 1){
      val scanUser = readLine("\nPlease enter your username: ")
      val scanPass = readLine("Please enter your password: ")
      val credentials = spark.sql(s"SELECT id FROM user WHERE username = '$scanUser' AND password = '$scanPass'")
      try{
        //If credentials are successfully found on user database, exits while loop with code 1
        credentials.head().getInt(0)
        println("Logged in successfully")
        validCred = 1

      }
      catch{
        //If credentials are not found on user database, user can choose to try again or exit the while loop with code 0
        case _: NoSuchElementException => println("Invalid credentials")
          while(!exitCheck){
            var scanMenu = readLine("Would you like to exit the program? [Y]es/[N]o ")
            scanMenu match{
              case y if y matches "(?i)Y" => validCred = 0; exitCheck = true
              case n if n matches "(?i)N" => println("Ok, returning to login. "); exitCheck = true
              case _ =>  println("Invalid input. Please input [Y]es/[N]o ")
            }
          }

      }
    }

    //Return true if user logged in successfully, false if they failed to login
    if(validCred == 1) true else false
  }

  //Get input from user for year, check if input is valid, returns year
  def yearInput(): Int={

    var isFormat = false
    var callYear = 0
    val currentYear = LocalDate.now.getYear

    while(!isFormat){
      try{
        callYear = readLine("Please enter the year you wish to see data for: ").toInt
        if(callYear < 1979 | callYear > currentYear){
          println(s"Year out of range. Please enter year between 1979 and $currentYear")
        }
        else {
          isFormat = true
        }
      }
      catch{
        case _: NumberFormatException => println("Year improperly formatted. Please enter a 4 digit integer")
      }
    }
    return callYear
  }
}
