import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{SparkSession, _}


case class Flight(YEAR: Int, MONTH: Int, DAY: Int, DAY_OF_WEEK: Int, AIRLINE: String, FLIGHT_NUMBER: Int, TAIL_NUMBER: String, ORIGIN_AIRPORT: String, DESTINATION_AIRPORT: String, SCHEDULED_DEPARTURE: String, DEPARTURE_TIME: String, DEPARTURE_DELAY: Int, TAXI_OUT: Int, WHEELS_OFF: Int, SCHEDULED_TIME: Int, ELAPSED_TIME: Int, AIR_TIME: Int, DISTANCE: Int, WHEELS_ON: Int, TAXI_IN: Int, SCHEDULED_ARRIVAL: Int, ARRIVAL_TIME: String, ARRIVAL_DELAY: Int, DIVERTED: Int, CANCELLED: Int, CANCELLATION_REASON: String, AIR_SYSTEM_DELAY: Int, SECURITY_DELAY: Int, AIRLINE_DELAY: Int, LATE_AIRCRAFT_DELAY: Int, WEATHER_DELAY: String) extends Serializable {}

object Main extends App {
  override def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FlightsConsumer").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    //       sc.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    val host = "127.0.0.1"
    val clusterName = "Test Cluster"
    val keyspace = "flight_data"
    val tableName = "flights"

    spark.setCassandraConf(clusterName, CassandraConnectorConf.ConnectionHostParam.option(host))

    import spark.implicits._

    val schema = Seq[Flight]().toDF.schema
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 5000)
      //    .trigger(Trigger.ProcessingTime("100 seconds"))
      .load()

    val df2 = df.selectExpr("CAST(value AS STRING)")

    val ncols = 31
    val colArr = Array("YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER", "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "DEPARTURE_DELAY", "TAXI_OUT", "WHEELS_OFF", "SCHEDULED_TIME", "ELAPSED_TIME", "AIR_TIME", "DISTANCE", "WHEELS_ON", "TAXI_IN", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME", "ARRIVAL_DELAY", "DIVERTED", "CANCELLED", "CANCELLATION_REASON", "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")


    val dataTypeArr = Array("integer", "integer", "integer", "integer", "string", "integer", "string", "string", "string", "string", "string", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "integer", "string", "integer", "integer", "integer", "integer", "string")

    val df3 = df2.as[String].map(x => x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map { case "" => "0"; case x => x })
    val selectCols = (0 until ncols).map(i => $"value" (i).as(colArr(i)).cast(dataTypeArr(i)))
    val df4 = df3.select(selectCols: _*)

    val query = df4.select("AIRLINE", "SCHEDULED_DEPARTURE", "FLIGHT_NUMBER", "TAIL_NUMBER", "AIRLINE_DELAY", "AIR_SYSTEM_DELAY", "AIR_TIME", "ARRIVAL_DELAY", "ARRIVAL_TIME", "CANCELLATION_REASON", "CANCELLED", "DAY", "DAY_OF_WEEK", "DEPARTURE_DELAY", "DEPARTURE_TIME", "DESTINATION_AIRPORT", "DISTANCE", "DIVERTED", "ELAPSED_TIME", "LATE_AIRCRAFT_DELAY", "MONTH", "ORIGIN_AIRPORT", "SCHEDULED_ARRIVAL", "SCHEDULED_TIME", "SECURITY_DELAY", "TAXI_IN", "TAXI_OUT", "WEATHER_DELAY", "WHEELS_OFF", "WHEELS_ON", "YEAR")
      .writeStream.foreachBatch {
      (batchDF: DataFrame, batchId: Long) =>
        (batchDF.write // Use Cassandra batch data source to write streaming out
          .cassandraFormat(tableName, keyspace)
          .option("cluster", clusterName)
          .mode("append")
          .save()
          )


    }
      .outputMode("update")
      .start()


    query.awaitTermination()

  }
}