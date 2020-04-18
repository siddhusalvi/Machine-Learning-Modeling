import java.io.FileWriter
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import settings.Configuration
import scala.collection.mutable.ListBuffer

object stockmodel {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName("Stock_prediction").master("local").getOrCreate()

      //setting hadoop configuration
      val config = new Configuration
      config.Configure(spark)

      //Getting s3 file contents
      val bucket = "marketdatas"
      val file = "stockdata.csv"
      val path = "C:\\Users\\Siddesh\\IdeaProjects\\stockdata\\src\\resources\\"

      //saving s3 file locally
      downloadFile(spark, bucket, file, path)
      val input_path = path + file

      //creating list to inject data into python script
      val command = "python " + path + "stockprice.py"
      val pkl_file = "output.pkl"

      val cli_input = spark.sparkContext.parallelize(List(input_path, path + pkl_file))
      val operation = cli_input.pipe(command)
      val python_output = operation.collect()

      //printing python script output
      python_output.foreach(println(_))

      //saving pkl file on s3
      val source = new Path(path + pkl_file)
      val srcFs = FileSystem.get(source.toUri, spark.sparkContext.hadoopConfiguration)
      val dest = new Path("s3a://" + bucket + "//")
      val dstFs = FileSystem.get(dest.toUri, spark.sparkContext.hadoopConfiguration)
      FileUtil.copy(srcFs, source, dstFs, dest, false, false, spark.sparkContext.hadoopConfiguration)
    } catch {

      case exception1 : NoSuchMethodError => println(exception1)
      case exception2: ClassNotFoundException => println(exception2)
      case _ => println("Unknown Error occured!")
    }
  }

  def downloadFile(spark: SparkSession, bucket: String, file: String, path: String): Unit = {
    val text = spark.read.textFile("s3a://" + bucket + "/" + file)
    var data = ListBuffer[String]()
    for (line: String <- text.collect()) {
      data += line
    }
    val input_path = path + file
    //Saving csv file on local storage
    val writer = new FileWriter(input_path, true)
    data.foreach(c => writer.write(c.toString + "\n"))
    writer.close()
  }
}