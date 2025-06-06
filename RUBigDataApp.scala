package org.rubigdata

import org.apache.spark.sql.SparkSession

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup
import org.apache.commons.lang3.StringUtils
import org.jsoup.nodes.{Document,Element}
import collection.JavaConverters._
import org.apache.spark.sql.functions._

object RUBigDataApp {
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf()
      .setAppName("RUBigDataApp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord]))

    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val warcfile = s"/opt/hadoop/rubigdata/CC-MAIN-20210410105831-20210410135831-00639.warc.gz"

    val warcs = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcGzInputFormat],             // InputFormat
              classOf[NullWritable],                  // Key
              classOf[WarcWritable]                   // Value
    ).cache()

    val filteredWarcs = warcs.map{ wr => wr._2 }.
                        filter{ _.isValid() }.
                        map{ _.getRecord() }.
                        filter{ _.getHeader().getHeaderValue("WARC-Type") == "response" }.
                        map { wr => {
                            val url = wr.getHeader().getUrl()
                            (wr, url)
                            }
                        }.
                        map { wr => (wr._1.getHttpStringBody(), wr._2) }.
                        filter { _._1.length > 0 }.
                        map { wr => (Jsoup.parse(wr._1), wr._2) }.
                        map { wr => {
                                val title = wr._1.title().toLowerCase()
                                val url = wr._2
                                val content = wr._1.body().text().toLowerCase()
                                val covidCount = StringUtils.countMatches(content, "covid")
                                val covid19Count = StringUtils.countMatches(content, "covid-19")
                                val coronaCount = StringUtils.countMatches(content, "corona")
                                val pandemicCount = StringUtils.countMatches(content, "pandemic")
                                val sarsCount = StringUtils.countMatches(content, "sars-cov-2")
                                (title, url, covidCount+covid19Count+coronaCount+pandemicCount+sarsCount)
                            } 
                        }.
                        filter{ wr => wr._1.length > 0 && wr._2.length > 0 }

    val DF = filteredWarcs.toDF("title", "url", "count")
    DF.createOrReplaceTempView("DataFrame")

    val filteredDF = spark.sql(
      """
      SELECT * FROM DataFrame
      WHERE title LIKE '%covid%'
      OR title LIKE '%covid-19%'
      OR title LIKE '%corona%'
      OR title LIKE '%pandemic%'
      OR title LIKE '%sars-cov-2%'
      OR url LIKE '%covid%'
      OR url LIKE '%covid-19%'
      OR url LIKE '%corona%'
      OR url LIKE '%pandemic%'
      OR url LIKE '%sars-cov-2%'
      OR count > 0
      """
    )
    
    filteredDF.createOrReplaceTempView("FilteredDataFrame")
    filteredDF.show(10)
  }
}
