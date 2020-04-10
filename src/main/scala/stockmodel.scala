import java.io.FileWriter
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object stockmodel {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("s3_file").master("local").getOrCreate()

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
    val writer = new FileWriter("C:\\Users\\Siddesh\\Desktop\\" + file, true)
    data.foreach(c => writer.write(c.toString + "\n"))
    writer.close()
  }
}