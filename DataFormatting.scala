import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql

object DataFormatting {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Data Formatting")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val batchId = args(0)
    val create_hive_table = """CREATE TABLE IF NOT EXISTS project.formatted_input
                             (
                             User_id STRING,
                             Song_id STRING,
                             Artist_id STRING,
                             Timestamp STRING,
                             Start_ts STRING,
                             End_ts STRING,
                             Geo_cd STRING,
                             Station_id STRING,
                             Song_end_type INT,
                             Like INT,
                             Dislike INT
                             )
                             PARTITIONED BY
                             (batchid INT)
                             ROW FORMAT DELIMITED
                             FIELDS TERMINATED BY ','
                             """

    val load_mob_data = s"""LOAD DATA LOCAL INPATH 'file:///home/acadgild/examples/music/data/mob/file.txt'
                             INTO TABLE project.formatted_input PARTITION (batchid='$batchId')"""

    val load_web_data = s"""INSERT INTO project.formatted_input
                             PARTITION(batchid='$batchId')
                             SELECT user_id, 
                             song_id,
                             artist_id,
                             unix_timestamp(timestamp,'yyyy-MM-dd HH:mm:ss') AS timestamp,
                             unix_timestamp(start_ts,'yyyy-MM-dd HH:mm:ss') AS start_ts,
                             unix_timestamp(end_ts,'yyyy-MM-dd HH:mm:ss') AS end_ts,
                             geo_cd,
                             station_id,
                             song_end_type,
                             like,
                             dislike
                             FROM web_data
                             """


    try {
         val xmlData = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "record").load("file:///home/acadgild/examples/music/data/web/file.xml")
         xmlData.createOrReplaceTempView("web_data")

         sqlContext.sql(create_hive_table)
         sqlContext.sql(load_mob_data)
         sqlContext.sql(load_web_data)
       }
      catch{
       case e: Exception=>e.printStackTrace()
      }
 }
}

