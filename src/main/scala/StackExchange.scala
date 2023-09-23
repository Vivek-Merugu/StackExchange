import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



object StackExchange {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println("=====hello world==========")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Initialize Spark session
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("FXEngine")
      .getOrCreate()

    // Read the CSV file containing orders
    val ordersDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("src\\main\\resources\\exampleOrders-testcase2.csv")
      .toDF("OrderID", "UserName", "OrderTime", "OrderType", "Quantity", "Price").withColumn("status",lit("open"))

    ordersDF.show(truncate=false)

    val buyOrder = ordersDF.filter("OrderType=='BUY'").select(col("OrderID").alias("BOrderID"),col("UserName").alias("BUserName"),
      col("OrderTime").alias("BOrderTime"),col("OrderType").alias("BOrderType"),col("Quantity"),col("Price").alias("BPrice"),
      col("status").alias("BStatus"))
    buyOrder.show(truncate = false)

    val sellOrder = ordersDF.filter("OrderType=='SELL'").select(col("OrderID").alias("SOrderID"),col("UserName").alias("SUserName"),
      col("OrderTime").alias("SOrderTime"),col("OrderType").alias("SOrderType"),col("Quantity"),col("Price").alias("SPrice"),
      col("status").alias("SStatus"))
    sellOrder.show(truncate = false)

    //test case 2 when sell order with the same Quantity occurs more than once
    val windowSpec = Window.partitionBy("BOrderID").orderBy(asc("SOrderTime"))


    val matchOrdersBasedOnQuantity = buyOrder.join(sellOrder,Seq("Quantity"),"inner")
      .withColumn("rank", dense_rank().over(windowSpec))
      .where(col("rank") === 1)
      .drop("rank")

    matchOrdersBasedOnQuantity.show(truncate = false)

    val matchOrdersBasedOnQuantity_OrderTime = matchOrdersBasedOnQuantity.withColumn("OrderTime",
      when(col("BOrderTime") > col("SOrderTime"), col("BOrderTime"))
        .otherwise(col("SOrderTime")))

    matchOrdersBasedOnQuantity_OrderTime.show(truncate = false)

    val matchOrdersBasedOnQuantity_OrderTime_latestOrderID_oldOrderID = matchOrdersBasedOnQuantity_OrderTime.withColumn("latestOrderID",
      when(col("BOrderID") > col("SOrderID"),col("BOrderID"))
        .otherwise(col("SOrderID")))
      .withColumn("oldOrderID",
        when(col("BOrderID") < col("SOrderID"),col("BOrderID"))
          .otherwise(col("SOrderID")))

    matchOrdersBasedOnQuantity_OrderTime_latestOrderID_oldOrderID.show(truncate = false)

    val matchOrdersBasedOnQuantity_OrderTime_latestOrderID_oldOrderID_price = matchOrdersBasedOnQuantity_OrderTime_latestOrderID_oldOrderID.withColumn("Price",
      when(col("SOrderTime")<col("BOrderTime"),col("SPrice"))
        .otherwise(col("BPrice")))

    val matchOrder = matchOrdersBasedOnQuantity_OrderTime_latestOrderID_oldOrderID_price.select("latestOrderID","oldOrderID","OrderTime","Quantity","Price")

    matchOrder.show(truncate = false)

    //matchOrder.write.option("header","false").csv("src\\main\\resources\\outputExampleMatches.csv")


  }
}