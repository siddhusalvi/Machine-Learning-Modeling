package settings
import org.apache.spark.sql.SparkSession

class Configuration {
  def Configure(obj: SparkSession): Unit = {
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    obj.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", System.getenv("key"))
    obj.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", System.getenv("secretkey"))
    obj.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
    obj.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    obj.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
    obj.sparkContext.hadoopConfiguration.set("spark.network.timeout", "600s")
    obj.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  }
}
