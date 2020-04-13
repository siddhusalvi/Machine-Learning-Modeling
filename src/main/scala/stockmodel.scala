import java.io.FileWriter
import org.apache.spark.sql.SparkSession
import settings.Configuration
import scala.collection.mutable.ListBuffer

object stockmodel {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Stock_prediction").master("local").getOrCreate()
    val bucket = "marketdatas"
    val file = "stockdata.csv"
    val config = new Configuration
    config.Configure(spark)
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
    val command = script
    //creating dummy RDD to pipe python script
    val dummy_data = spark.sparkContext.parallelize(List(output_path, pkl_file))
    val operation = dummy_data.pipe(command)
    val python_output = operation.collect()
    //printing python script output
    python_output.foreach(println(_))
  }
}