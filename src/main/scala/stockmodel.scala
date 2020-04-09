import org.apache.spark.{SparkConf, SparkContext}

object stockmodel {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("s3wordcounter").setMaster("local")
    val sc = new SparkContext(conf)

    val bucket = "marketdatas"
    val file = "stockdata.csv"

    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    sc.hadoopConfiguration.set("fs.s3a.access.key", key)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretkey)
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    val text = sc.textFile("s3a://" + bucket + "/" + file)

  }
}
