import org.apache.spark.SparkContext

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HTableDescriptor,HColumnDescriptor}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Put,HTable}
import org.apache.log4j._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result

object SparkHBaseTest {

  def main(args: Array[String]) {
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "SparkHBaseTest")

    println("spark hbase ---> 1")

    val conf = HBaseConfiguration.create()
   val tablename = "SparkHBasesTable1"
    conf.set(TableInputFormat.INPUT_TABLE,tablename)
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tablename)){
      print("creating table:"+tablename+"\t") //creates a table if it does not exists
      val tableDescription = new HTableDescriptor(tablename)
      tableDescription.addFamily(new HColumnDescriptor("cf".getBytes()));
      admin.createTable(tableDescription);
    } else {
      print("table already exists") //prints an error if table exists already
    }

    val table = new HTable(conf,tablename);
    for(x <- 1 to 10){ //For inserting 10 values inside the table
      var p = new Put(new String("row" + x).getBytes());
      p.add("cf".getBytes(),"column".getBytes(),new String("value" + x).getBytes());
      table.put(p);
      print("Data Entered In Table")
    }
    //below is the code which converts the API code into spark rdd 
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],classOf[Result])
    print("RecordCount->>"+hBaseRDD.count()) //gives the total number of rows
    sc.stop()
  }
}
