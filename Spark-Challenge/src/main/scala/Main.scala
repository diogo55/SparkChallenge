import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main   {

  val parserHelper = new ParserHelper()

  var df_3_rows: Array[(String, Array[String], Double, Long, Double, String, String, Double, String, Array[String], String, String, String)] = Array()

  var df_5_rows: Array[(String,(Int,Double,Double))] = Array()
  /*
  * return parsed elements of CSV.
  * Value is composed by polarity and value "1" (to help calculate number of appearances)
   */
  def getSentimentPolarity(line:String): (String,(Double,Int)) = {
    val fields = line.split(",")

    val appName = fields(0).toString
    val polarity = parserHelper.parseDouble(fields(3))

    (appName,(polarity,1))
  }

  /*
   * Save Genres information.
   */
  def getGenreAttributes(genre:Seq[String], rating: Any, sentiment: Any) = {
    genre.foreach(genreName =>
      df_5_rows = df_5_rows :+ (genreName.toString,(1,parserHelper.parseDouble(rating.toString),parserHelper.parseDouble(sentiment.toString)))
    )
  }

  /*
  * Function responsible for the part1 of the challenge
   */
  def part1(sc: SparkContext,spark:SparkSession):DataFrame = {
    //read file
    val lines = sc.textFile("src/main/resources/googleplaystore_user_reviews.csv")

    val header = lines.first
    //remove header and parse rows
    val rdd = lines.filter(row => !row.contains(header)).map(getSentimentPolarity)
    //join values of each key
    val totals = rdd.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
    //calculate Average_Sentiment_Polarity
    val average = totals.mapValues(x => x._1/x._2).collect().sorted.toSeq
    val columnNames = Seq("App","Average_Sentiment_Polarity")
    // Create Dataframe with header
    spark.createDataFrame(average).toDF(columnNames:_*)

  }

  //filter applications by rating
  def part2(sc: SparkContext,spark:SparkSession,rating:Int) = {
    val lines = spark.read.option("header",true).csv("src/main/resources/googleplaystore.csv")
    lines.filter(lines("Rating") >= rating).orderBy(desc("Rating"))
  }

  /*
 * Function responsible for the part3 of the challenge
  */
  def part3(sc: SparkContext,spark:SparkSession): DataFrame = {
    var df = spark.read.option("header",true).csv("src/main/resources/googleplaystore.csv")
    //order by name to treat duplicates
    df = df.orderBy("App")
    var name: String = "-1"
    var maxReviews: Long = -1
    var categories: Array[String] = Array()
    var rating: Double = -1
    var size : Double = -1
    var installs : String = ""
    var type_ : String = ""
    var price : Double = -1
    var content : String = ""
    var genres : Array[String] = Array()
    var update : String = ""
    var current : String = ""
    var minimum: String = ""

    df.foreach(f=>
      if (name==f.get(0)) {
        //if new category, add to categories
        if (!categories.contains(f.get(1).toString)) {
          categories :+ f.get(1).toString
        }
        val reviews = parserHelper.parseLong(f.get(3).toString)
        if (maxReviews< reviews) {
          //update values of entry
          maxReviews = reviews
          rating = parserHelper.parseDouble(f.get(2).toString)
          size = parserHelper.calculateMB(f.get(4).toString)
          installs = parserHelper.parseString(f.get(5))
          type_ = parserHelper.parseString(f.get(6))
          price = parserHelper.getEuroPrice(f.get(7).toString)
          content = parserHelper.parseString(f.get(8))
          genres = f.get(9).toString.split(";")
          update = parserHelper.StringToDate(f.get(10).toString)
          current = parserHelper.parseString(f.get(11))
          minimum = parserHelper.parseString(f.get(12))
        }
      } else {
        //if not first line processed, insert row
        if (name!="-1") {
          df_3_rows = df_3_rows :+ (name,categories,rating,maxReviews,size,installs,type_,price,content,genres,update,current,minimum)
        }
        //update values to new entry
        name=f.get(0).toString
        categories = Array(f.get(1).toString)
        maxReviews = parserHelper.parseLong(f.get(3).toString)
        rating = parserHelper.parseDouble(f.get(2).toString)
        size = parserHelper.calculateMB(f.get(4).toString)
        installs = parserHelper.parseString(f.get(5))
        type_ = parserHelper.parseString(f.get(6))
        price = parserHelper.getEuroPrice(f.get(7).toString)
        content = parserHelper.parseString(f.get(8))
        genres = f.get(9).toString.split(";")
        update = parserHelper.StringToDate(f.get(10).toString)
        current = parserHelper.parseString(f.get(11))
        minimum = parserHelper.parseString(f.get(12))
      }
    )
    val columnNames = Seq("App","Categories","Rating","Reviews","Size","Installs","Type","Price","Content_Rating","Genres","Last_Updated","Current_Version","Minimum_Android_Version")
   // create DataFrama with header
    spark.createDataFrame(df_3_rows.toSeq).toDF(columnNames:_*)
  }

  /*
 * Function responsible for the part5 of the challenge
  */
  def part5(sc: SparkContext,spark: SparkSession,df: DataFrame) = {
    // get information of all dataframe Rows
    df.foreach(f=> getGenreAttributes(f.getAs[Seq[String]](9),f.get(2),f.get(13)))
    //transform df_5_rows in rdd
    val rdd = sc.parallelize(df_5_rows.toSeq)
    //Aggregate by genre and calculate total values
    val totals = rdd.reduceByKey((x,y) => (x._1+y._1,x._2+y._2,x._3+y._3))
    //calculate averages
    val average = totals.mapValues(x => (x._1,x._2/x._1,x._3/x._1)).collect()
    var array : Array[(String,Int,Double,Double)] = Array()
    //Create new array with the disposition of information needed
    for ( (x,y) <- average ) {
      array = array :+ (x,y._1,y._2,y._3)
    }
    val columnNames = Seq("Genre","Count","Average_Rating","Average_Sentiment_Polarity")
    //create Dataframe with header
    spark.createDataFrame(array.toSeq).toDF(columnNames:_*)
  }

  def main(args: Array[String]): Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext

    val df_1 = part1(sc,spark).orderBy(asc("App"))

    val df_2 = part2(sc,spark,4)
    //save dataframe
    df_2.repartition(1).write.options(Map("header"->"true", "delimiter"->"§")).mode("overwrite").csv("target/best_apps.csv")

    val df_3 = part3(sc,spark).orderBy(asc("App"))

    val df_4 = df_3.join(df_1,df_3("App") <=> df_1("App"),"right").drop(df_1("App")).na.drop(Seq("App"))
    df_4.write.option("header",true).option("compression","gzip").mode("overwrite").parquet("target/googleplaystore_cleaned")

    val df_5 = part5(sc,spark,df_4)
    df_5.write.option("header",true).option("compression","gzip").mode("overwrite").parquet("target/googleplaystore_metrics")

    }
}
