import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by rohith on 11/6/15.
 */


case class Line( l_orderkey : Int,
                 l_partkey : Int,
                 l_suppkey : Int,
                 l_linenumber : Int ,
                 l_quantity : Double,
                 l_extendedprice: Double,
                 l_discount : Double,
                 l_tax : Double,
                 l_returnflag : String,
                 l_linestatus : String,
                 l_shipdate : String,
                 l_comitdate : String,
                 l_receiptdate : String,
                 l_shipinstruct : String ,
                 l_shipmode : String,
                 l_comment: String  )

object queryone {
  def main(args: Array[String]) {
//    val sparkConf = new SparkConf()
//      .set("spark.executor.uri","hdfs://Sigmoid/executors/spark/spark-1.2.0-bin-hadoop2.4.tgz") //spark executor stored in cluster's hadoop
//      .setMaster("mesos://zk://10.136.129.94:2181,10.178.143.90:2181,10.69.46.193:2181/mesos")
//      .setJars(List(""))
//      .setSparkHome("/home/hadoop/spark-install")
//      .setAppName("Counting_lines") //Application name


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("local test").setJars(List("target/scala-2.10/csv_to_json_2.10-1.0.jar")).setSparkHome("/home/rohith/spark-1.3.1")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile("data.txt").flatMap(_.split("\n"))
    val lineitem = data.map(_.split("\\|")).map( p => Line(p(0).toInt,p(1).toInt,p(2).toInt,p(3).toInt,p(4).toDouble,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8),p(9),p(10),p(11),p(12),p(13),p(14),p(15))).toDF()
    lineitem.registerTempTable("lineitem")
    //val countdata = sqlContext.sql("select  l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,  avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc,count(*) as count_order from lineitem  group by l_returnflag,l_linestatus order by  l_returnflag, l_linestatus")
    val countdata = sqlContext.sql("select l_shipdate from lineitem where l_shipdate < '2000-10-10'   ")
    countdata.show()

  }

}
