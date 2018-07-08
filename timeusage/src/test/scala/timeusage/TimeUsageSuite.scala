package timeusage

import java.io.Serializable

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage.spark

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {


  case class KV(key: Int, value: String) //extends java.io.Serializable

  val schema = StructType(List(StructField("key", IntegerType, nullable = false), StructField("value", StringType, nullable = false)))
  val sc = TimeUsage.spark.sparkContext


  val kv1 = KV(1, "one")
  val kv2 = KV(2, "two")
  val kv3 = KV(3, "three")
  val kv4 = KV(4, "four")
  val list = List(kv1, kv2, kv3, kv4)

  test("creates data frame") {
    val rows =  list.map(kv => Row.fromSeq(List(kv.key, kv.value)))
    val rowsRdd = sc.parallelize(rows)
    val df = spark.createDataFrame(rowsRdd, schema)
    df.show()
  }
}
