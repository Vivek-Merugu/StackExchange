import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



object StackExchange {

  def main(args: Array[String]): Unit = {

    val inputFilePath = args(0)
    val outputFilePath = args(1)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Initialize Spark session
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("FXEngine")
      .getOrCreate()

    // Read the CSV file containing orders
    //test case 4: combination of test 2 and 3
    val ordersDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(inputFilePath)
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
    val windowSpecSell = Window.partitionBy("BOrderID").orderBy(asc("SOrderTime"))

    //test case 3 when buy order with the same Quantity occurs more than once
    val windowSpecBuy = Window.partitionBy("SOrderID").orderBy(asc("BOrderTime"))


    val matchOrdersBasedOnQuantity = buyOrder.join(sellOrder,Seq("Quantity"),"inner")
      .withColumn("rankSell", dense_rank().over(windowSpecSell))
      .where(col("rankSell") === 1)
      .drop("rankSell")
      .withColumn("rankBuy", dense_rank().over(windowSpecBuy))
      .where(col("rankBuy") === 1)
      .drop("rankBuy")


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
    matchOrder.write.option("header","false").csv(outputFilePath)


    //closing  the orders which are match based on quantity condition
    // Create a broadcast variable for faster lookup
    val latestOrderIDSet = spark.sparkContext.broadcast(matchOrder.select("latestOrderID").rdd.map(r => r.getInt(0)).collect.toSet)
    val oldOrderIDSet = spark.sparkContext.broadcast(matchOrder.select("oldOrderID").rdd.map(r => r.getInt(0)).collect.toSet)


    // Function to update the status column based on the latestOrderID or oldOrderID
    def updateStatusColumn(df: DataFrame, idColumnName: String, statusColumnName: String): DataFrame = {
      df.withColumn(statusColumnName, when(col(idColumnName).isin(latestOrderIDSet.value.toSeq: _*) || col(idColumnName).isin(oldOrderIDSet.value.toSeq: _*), "close").otherwise(col(statusColumnName)))
    }

    // Update the "BStatus" column in the "buy" dataframe
    val updatedBuyDF = updateStatusColumn(buyOrder, "BOrderID", "BStatus")

    // Update the "SStatus" column in the "sell" dataframe
    val updatedSellDF = updateStatusColumn(sellOrder, "SOrderID", "SStatus")

    // Show the updated dataframes
    updatedBuyDF.show()
    updatedSellDF.show()

    // Filter the records where "SStatus" is "open"
    val buyBookingOrderDF = updatedSellDF.filter(col("SStatus") === "open")
    val sellBookingOrderDF = updatedBuyDF.filter(col("BStatus") === "open")

    buyBookingOrderDF.show(truncate = false)
    sellBookingOrderDF.show(truncate = false)




  }
}
