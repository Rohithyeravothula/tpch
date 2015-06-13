import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


case class Part(p_partkey       : Int   ,
                p_name          : String   ,
                p_mfgr          : String   ,
                p_brand         : String      ,
                p_type          : String   ,
                p_size          : Int   ,
                p_container     : String ,
                p_retailprice   : Double,
                p_comment       : String )
case class Supplier(s_suppkey : Int  ,
                    s_name  : String ,
                    s_address : String,
                    s_nationkey : Int ,
                    s_phone     : String,
                    s_acctbal   : Double,
                    s_comment    : String)
case class Partsupp(
                      ps_partkey : Int,
                      ps_suppkey : Int,
                      ps_availqty : Int ,
                      ps_supplycost : Double,
                      ps_comment   : String
                     )
case class Nation(
                    N_NATIONKEY : Int ,
                    N_NAME : String   ,
                    N_REGIONKEY : Int ,
                    N_COMMENT  : String
                   )
case class Region(
                    R_REGIONKEY : Int,
                    R_NAME  : String ,
                    R_COMMENT : String
                   )

object querytwo {
  def main(args: Array[String]) {
    /*val sparkConf = new SparkConf()
      .set("spark.executor.uri","hdfs://Sigmoid/executors/spark/spark-1.2.0-bin-hadoop2.4.tgz") //spark executor stored in cluster's hadoop
      .setMaster("mesos://zk://10.136.129.94:2181,10.178.143.90:2181,10.69.46.193:2181/mesos")
      .setJars(List(""))
      .setSparkHome("/home/hadoop/spark-install")
      .setAppName("tpch2")*/

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("query2_test").setJars(List("target/scala-2.10/csv_to_json_2.10-1.0.jar")).setSparkHome("/home/rohith/spark-1.3.1")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val datapart = sc.textFile("data/part.tbl").flatMap(_.split("\n"))
    import sqlContext.implicits._
    val  datapartline = datapart.map(_.split("\\|")).map( p => Part(p(0).toInt,p(1),p(2),p(3),p(4),p(5).toInt,p(6),p(7).toDouble,p(8))).toDF()
    datapartline.registerTempTable("part")
    datapartline.cache()

    val datasupplier = sc.textFile("data/supplier.tbl").flatMap(_.split("\n"))
    val datasupplierline = datasupplier.map(_.split("\\|")).map( p => Supplier(p(0).toInt, p(1), p(2), p(3).toInt, p(4) ,p(5).toDouble,p(6))).toDF()
    datasupplierline.registerTempTable("supplier")
    datasupplier.cache()

    val datapartsupp = sc.textFile("data/partsupp.tbl").flatMap(_.split("\n"))
    val datapartsuppline  = datapartsupp.map(_.split("\\|")).map(p => Partsupp(p(0).toInt,p(1).toInt,p(2).toInt,p(3).toDouble,p(4))).toDF()
    datapartsuppline.registerTempTable("partsupp")
    datapartsuppline.cache()

    val datanation = sc.textFile("data/nation.tbl").flatMap(_.split("\n"))
    val datanationline = datanation.map(_.split("\\|")).map(p => Nation(p(0).toInt,p(1).toString,p(2).toInt,p(3).toString)).toDF()
    datanationline.registerTempTable("nation")
    datanationline.cache()

    val dataregion = sc.textFile("data/region.tbl").flatMap(_.split("\n"))
    val dataregionline = dataregion.map(_.split("\\|")).map(p => Region(p(0).toInt,p(1),p(2))).toDF()
    dataregionline.registerTempTable("region")
    dataregionline.cache()


    //val finalresult = sqlContext.sql("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation,region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = :1 and p_type like '%:2' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = ':3' and ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = ':3') order by s_acctbal desc, n_name, s_name, p_partkey")
    val finalresult = sqlContext.sql("select s_acctbal from supplier ")
    finalresult.show()

  }

}
