package org.alitouka.spark.dbscan.util.io

import com.google.common.reflect.TypeToken
import com.google.gson.Gson
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray.ofDouble
import org.alitouka.spark.dbscan._
import org.apache.spark.rdd.RDD
import org.alitouka.spark.dbscan.spatial.Point
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Contains functions for reading and writing data
  *
  */
object IOHelper {

  /** Reads a dataset from a CSV file. That file should contain double values separated by commas
    *
    * @param sc   A SparkContext into which the data should be loaded
    * @param path A path to the CSV file
    * @return A [[org.alitouka.spark.dbscan.RawDataSet]] populated with points
    */
  def readDataset(sc: SparkContext, path: String): RawDataSet = {
    val rawData = sc.textFile(path, 10)

    rawData.map(
      line => {
        val arr = line.split(separator)
        new Point(new PointCoordinates(arr.slice(1, 3).map(_.toDouble)), pointId = arr(0).toLong)
      }
    )
    //    sc.textFile(path, 10).mapPartitions(
    //      it => {
    //        val gson = new Gson()
    //        val result = new java.util.ArrayList[Point]
    //        it.flatMap(line => {
    //          val map: java.util.Map[String, Object] = gson.fromJson(line, new TypeToken[java.util.Map[String, Object]]() {}.getType)
    //          import scala.collection.JavaConversions._
    //          for (entry <- map.entrySet()) {
    //            var u = entry.getKey();
    //            val timeMap = entry.getValue.asInstanceOf[java.util.Map[String, Object]]
    //            val dataMap = timeMap.get("11").asInstanceOf[java.util.Map[String, Object]]
    //            result.add(new Point(pointId = u.toLong, dataMap.get("lng").asInstanceOf[Double], dataMap.get("lat").asInstanceOf[Double]))
    //          }
    //          result
    //        })
    //      }
    //    )

  }

  def readData(spark: SparkSession, path: String): RawDataSet = {
    import spark.implicits._
    //  读取数据
    val data = spark.sparkContext.wholeTextFiles(path).flatMap(_._2.split("\n").filter(!_.equals("")))
      .map { line =>
        val columns = line.split(' ')
        val Array(uid, tag_id, weight, cat_id) = columns
        Data(uid.toLong, tag_id, weight.toDouble, cat_id)
      }

    data.map(_.uid).distinct().zipWithUniqueId()

    val filterData = spark.createDataFrame(data).filter($"cat_id" === "1042015:tagCategory_012")


    val distinctTag = filterData.select("tag_id").distinct().orderBy($"tag_id".desc).as[(String)].collect()
    //.rdd.distinct().zipWithUniqueId()
    val tagBd = spark.sparkContext.broadcast(distinctTag)

    filterData.select("uid", "tag_id", "weight").as[(Long, String, Double)].groupByKey { case (uid, tag_id, weight) => uid }
      .keyAs[(Long)]
      .mapGroups { case (uid, iter) =>
        val arr: Array[Double] = new Array[Double](tagBd.value.length)
        iter.foreach { case (uid, tag_id, weight) =>
          arr(tagBd.value.indexOf(tag_id)) = weight
        }
        (uid, arr)
      }.rdd.map { case (uid, arr) => new Point(new PointCoordinates(arr), pointId = uid) }

  }

  /** Saves clustering result into a CSV file. The resulting file will contain the same data as the input file,
    * with a cluster ID appended to each record. The order of records is not guaranteed to be the same as in the
    * input file
    *
    * @param model      A [[org.alitouka.spark.dbscan.DbscanModel]] obtained from Dbscan.train method
    * @param outputPath Path to a folder where results should be saved. The folder will contain multiple
    *                   partXXXX files
    */
  def saveClusteringResult(model: DbscanModel, outputPath: String) {

    model.allPoints.map(pt => {

      pt.pointId + separator + pt.coordinates.mkString(separator) + separator + pt.clusterId
    }).saveAsTextFile(outputPath)
  }


  def saveClusterPoint(model: DbscanModel, outputPath: String): Unit = {
    val rdd = model.clusteredPoints.map(pt => (pt.clusterId, pt.pointId)).groupByKey()
    rdd.cache()
    println("cluster size :" + rdd.count())
    println("total size :" + rdd.map({ case (a, b) => {
      b.size * (b.size - 1) / 2
    }
    }).sum())

    val resultRDD = rdd.flatMap({ case (a, b) => {
      val result = new java.util.ArrayList[String]()
      val list = b.toList
      var i = 0
      while (i < list.size) {
        var j = i + 1
        while (j < list.size) {
          if (list(i) > list(j)) result.add(list(j) + "," + list(i) + ",11," + 0.5)
          else result.add(list(i) + "," + list(j) + ",11," + 0.5)
          j += 1
        }
        i += 1
      }
      result
    }
    })


    rdd.map({ case (a, b) => {
      b.mkString(",")
    }
    }).saveAsTextFile(outputPath)
  }

  private[dbscan] def saveTriples(data: RDD[(Double, Double, Long)], outputPath: String) {
    data.map(x => x._1 + separator + x._2 + separator + x._3).saveAsTextFile(outputPath)
  }

  private def separator = ","

}
