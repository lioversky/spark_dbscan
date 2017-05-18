package org.alitouka.spark.dbscan

import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.commandLine._
import org.alitouka.spark.dbscan.util.debug.{Clock, DebugHelper}
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/** A driver program which runs DBSCAN clustering algorithm
  *
  */

case class Data(uid: Long, tag_id: String, weight: Double,cat_id:String)
object DbscanDriver {

  private[dbscan] class Args(var minPts: Int = DbscanSettings.getDefaultNumberOfPoints,
                             var borderPointsAsNoise: Boolean = DbscanSettings.getDefaultTreatmentOfBorderPoints)
    extends CommonArgs with EpsArg with NumberOfPointsInPartitionArg

  private[dbscan] class ArgsParser
    extends CommonArgsParser(new Args(), "DBSCAN clustering algorithm")
      with EpsArgParsing[Args]
      with NumberOfPointsInPartitionParsing[Args] {

    opt[Int]("numPts")
      .required()
      .foreach {
        args.minPts = _
      }
      .valueName("<minPts>")
      .text("TODO: add description")

    opt[Boolean]("borderPointsAsNoise")
      .foreach {
        args.borderPointsAsNoise = _
      }
      .text("A flag which indicates whether border points should be treated as noise")
  }


  def main(args: Array[String]): Unit = {

    val argsParser = new ArgsParser()

    if (argsParser.parse(args)) {

      val clock = new Clock()


      val conf = new SparkConf()
        .setMaster(argsParser.args.masterUrl)
        .setAppName("DBSCAN")
      //        .setJars(Array(argsParser.args.jar))

      if (argsParser.args.debugOutputPath.isDefined) {
        conf.set(DebugHelper.DebugOutputPath, argsParser.args.debugOutputPath.get)
      }

      val spark = SparkSession.builder().getOrCreate()


//      val data = IOHelper.readDataset(spark.sparkContext, argsParser.args.inputPath)
      val data = IOHelper.readData(spark,argsParser.args.inputPath)
      data.cache()
      val settings = new DbscanSettings()
        .withEpsilon(argsParser.args.eps)
        .withNumberOfPoints(argsParser.args.minPts)
        .withTreatBorderPointsAsNoise(argsParser.args.borderPointsAsNoise)
        .withDistanceMeasure(argsParser.args.distanceMeasure)

      val partitioningSettings = new PartitioningSettings(numberOfPointsInBox = argsParser.args.numberOfPoints)

      val clusteringResult = Dbscan.train(data, settings, partitioningSettings)

      println("noise size:"+ clusteringResult.noisePoints.count())
      IOHelper.saveClusteringResult(clusteringResult, argsParser.args.outputPath)


      clock.logTimeSinceStart("Clustering")
    }
  }
}
