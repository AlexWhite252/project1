import java.io.FileWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._
import java.time.LocalDate

object apiproject {
  def main(args: Array[String]): Unit = {

    //Sets up the spark environment
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")

    if (accountCheck(spark) == true) {
      if(login(spark) == true) {
        //clientID needed to connect to API, must be hardcoded to function
        val clientID = "ttxorkkbyio5zmvc5ctaueisv45fk1"
        //Call apiConnect to get an access token for further API calls
        val acTkn = apiConnect(clientID)

        var isFormat = false
        var callYear = 0
        val currentYear = LocalDate.now.getYear

        while(isFormat != true){
          try{
            callYear = readLine("Please enter the year you wish to see data for: ").toInt
            if(callYear < 1979 | callYear > currentYear){
              println(s"Year out of range. Please enter year between 1979 and ${currentYear}")
            }
            else {
              isFormat = true
            }
          }
          catch{
            case e: NumberFormatException => println("Year improperly formatted. Please enter a 4 digit integer")
          }
        }


        //The data we're querying from the API
        val call = s"fields id, aggregated_rating, name, genres.name, release_dates.human ; where aggregated_rating > 75 & aggregated_rating != 100 & release_dates.y = ${callYear} & category = 0; sort aggregated_rating desc; limit 500;"
        val call2 = "fields id, name,release_dates.human, aggregated_rating; where name = *\"Mario\"* & aggregated_rating != null; sort aggregated_rating desc; limit 500;"
        //Call the API and pass it the necessary clientID and access token as well as the specific query we want
        apiCall(clientID, acTkn, call)
        apiCall(clientID, acTkn, call2)

        spark.sql("CREATE TABLE IF NOT EXISTS user(id INT, username STRING, password STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'")

        var df1 = spark.read.option("Multiline", true).json(s"json/${call.substring(7, 17)}_out.json")
        df1.show()
        var df2 = spark.read.option("Multiline", true).json(s"json/${call2.substring(7, 17)}_out.json")
        df2.show()
      }
    }

    spark.close()
  }

  //Connects to the API using clientID and returns an accTkn
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
    //println(gameParsed.render(indent = 4))
    val writer = new FileWriter(s"json/${call.substring(7,17)}_out.json")
    ujson.writeTo(gameParsed, writer, 4)
    writer.flush()
    writer.close()
  }

  //Checks if user has an account, and if they don't allow them to create one.
  //Return true if they have an account, false if they don't
  def accountCheck(spark: SparkSession): Boolean={
    var accExist = 2
    var exitCheck = false

    while(accExist != 0 & accExist !=1 ){
      var scanAcc = readLine("Do you have an account? (Y/N) ")
      scanAcc match{
        //If user has an account, exit while loop with code 1
        case y if y matches "(?i)Y" => accExist = 1
        //If user does not have an account, check if they want to create one
        case n if n matches "(?i)N" =>
          scanAcc = readLine("Would you like to create an account? (Y/N) ")
          while(exitCheck != true){
            scanAcc match{
              case y if y matches "(?i)Y" =>
                val scanUser = readLine("Please enter your desired username: ")
                val scanPass = readLine("Please enter your desired password: ")
                val credentials = spark.sql(s"SELECT id FROM user WHERE username = '${scanUser}'")
                try{
                  //Check the user database if your desired username already exists, if it does then enter another
                  credentials.head().getInt(0)
                  println("Username already exists, enter another username")
                }
                catch{
                  //Check the user database if your desired username already exists, if it doesn't insert your user and pass into db
                  case e: NoSuchElementException => println("Creating account")
                  //Stretch goal - encrypt password, autoincrement userID
                  spark.sql(s"INSERT INTO user VALUES (2, '${scanUser}', '${scanPass}')")
                  accExist = 1
                  exitCheck = true
                }
              case n if n matches "(?i)N" =>
                println("Thank you. Now exiting program.")
                accExist = 0
                exitCheck = true
              case _ => println("Invalid input. Please input (Y/N)")
            }
          }

        case _ => println("Invalid input. Please input (Y/N)")
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
      val scanUser = readLine("Please enter your username: ")
      val scanPass = readLine("Please enter your password: ")
      val credentials = spark.sql(s"SELECT id FROM user WHERE username = '${scanUser}' AND password = '${scanPass}'")
      try{
        //If credentials are successfully found on user database, exits while loop with code 1
        credentials.head().getInt(0)
        println("Logged in successfully")
        validCred = 1

      }
      catch{
        //If credentials are not found on user database, user can choose to try again or exit the while loop with code 0
        case e: NoSuchElementException => println("Invalid credentials")
          while(exitCheck != true){
            var scanMenu = readLine("Would you like to exit the program? (Y/N) ")
            scanMenu match{
              case y if y matches "(?i)Y" => validCred = 0; exitCheck = true
              case n if n matches "(?i)N" => println("Ok, returning to login. "); exitCheck = true
              case _ =>  println("Invalid input. Please input (Y/N)")
            }
          }

      }
    }

    //Return true if user logged in successfully, false if they failed to login
    if(validCred == 1) true else false
  }
}
