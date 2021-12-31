package com.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, datediff, expr, unix_timestamp}

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("OlistProcessing")
      .getOrCreate();

    val orderSchema = new StructType()
      .add("order_purchase_timestamp", TimestampType, true)
      .add("order_approved_at", TimestampType, true)
      .add("order_delivered_carrier_date", TimestampType, true)
      .add("order_delivered_customer_date", TimestampType, true)
      .add("order_estimated_delivery_date", TimestampType, true)
      .add("order_id", StringType, true)
      .add("customer_id", StringType, true)
      .add("order_status", StringType, true)

    var dataFrameOrders = spark.read.format("csv").
      option("delimiter", ",").
      option("header", "true").
      schema(orderSchema).
      load("olist_orders_dataset.csv")


    val dataFrameCustomer = spark.read.format("csv").
      option("delimiter", ",").
      option("header", "true").
      load("olist_customers_dataset.csv")
    dataFrameOrders = dataFrameOrders.select(col("customer_id"), col("order_status"),
      col("order_delivered_customer_date"),
      col("order_purchase_timestamp"))
    dataFrameOrders = dataFrameOrders.filter(col("order_status") === "delivered")
    var customerDetails = dataFrameOrders.join(dataFrameCustomer, dataFrameOrders("customer_id") === dataFrameCustomer("customer_id"),
      "inner").drop(dataFrameOrders("customer_id"))
    customerDetails = customerDetails.withColumn("late_delivery",
      datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))
    customerDetails = customerDetails.filter(col("late_delivery") > 10)
    customerDetails.write.option("header", true)
      .option("delimiter", ",")
      .csv("customerdata")
  }


}
