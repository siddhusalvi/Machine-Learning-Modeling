import java.io.FileWriter

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object stockmodel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Stock_prediction").master("local").getOrCreate()

    val bucket = "marketdatas"
    val file = "stockdata.csv"

    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretkey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val text = spark.read.textFile("s3a://" + bucket + "/" + file)

    var data = ListBuffer[String]()
    for (line: String <- text.collect()) {
      data += line
    }

    val output_path = "C:\\Users\\Siddesh\\IdeaProjects\\stockdata\\src\\resources\\" + file
    val writer = new FileWriter(output_path, true)
    //Saving csv file on local storage
    data.foreach(c => writer.write(c.toString + "\n"))
    writer.close()

    val script = "python C:\\Users\\Siddesh\\IdeaProjects\\stockdata\\src\\resources\\stockprice.py"
    val pkl_file = "C:\\Users\\Siddesh\\IdeaProjects\\stockdata\\src\\resources\\output.pkl"
    val command = script + " " + output_path + " " + pkl_file
    //creating dummy RDD to pipe python script
    val dummy_data = spark.sparkContext.parallelize(List("a", "b"))
    val operation = dummy_data.pipe(command)
    operation.collect()
  }
}