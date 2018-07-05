import org.apache.spark.sql.SparkSession


object SparkHiveTest {
  
  def main (args: Array[String]) : Unit  = {
  
    val sparkSession = SparkSession.builder.master("local").appName("Assignment25-Task1").config("spark.sql.warehouse.dir","/user/hive/warehouse").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()
    val listOfDB = sparkSession.sqlContext.sql("show databases")
    listOfDB.show(8,false)
    println("test");
  }
}
