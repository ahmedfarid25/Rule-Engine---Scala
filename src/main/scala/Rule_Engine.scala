

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.format.DateTimeFormatter
import scala.io.{BufferedSource, Source}
import java.time.{Instant, LocalDate,LocalDateTime, ZonedDateTime}
import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.time.temporal.ChronoUnit


object Rule_Engine extends App {
  // Load the list of orders from the CSV file
  val source: BufferedSource = Source.fromFile("src/main/resources/TRX1000.csv")
  val orders: List[String] = source.getLines().drop(1).toList // Assuming the first line is a header and should be skipped
  source.close()

  def log_event(writer: PrintWriter, file: File, log_level: String, message: String): Unit = {
    writer.write(s"Timestamp: ${Instant.now()}\tLogLevel: ${log_level}\tMessage: ${message}\n")
    writer.flush()
  }


  def DateConverter(date_string: String, input_pattern: String): LocalDate = {
    val formatter = DateTimeFormatter.ofPattern(input_pattern)
    LocalDate.parse(date_string, formatter)
  }


  def extractOrderDetails(order: String) = {
    val fields = order.split(",")
    val timestamp = DateConverter(fields(0), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    val productName = fields(1)
    val expiryDate = DateConverter(fields(2), "yyyy-MM-dd")
    val quantity = fields(3).toInt
    val unitPrice = fields(4).toDouble
    val channel = fields(5)
    val paymentMethod = fields(6)
    (timestamp, productName, expiryDate, quantity, unitPrice, channel, paymentMethod)
  }




  def qualifyExpiryDate(order: String): Boolean = {
    val fields = order.split(",")
    val timestamp = ZonedDateTime.parse(fields(0))
    val expiryDate = LocalDate.parse(fields(2))
    val remainingDays = ChronoUnit.DAYS.between(timestamp.toLocalDate(), expiryDate)
    remainingDays <= 29 && remainingDays >= 1
  }


  def discountExpiryDate(order: String): Double = {
    val fields = order.split(",")
    val timestamp = ZonedDateTime.parse(fields(0))
    val expiryDate = LocalDate.parse(fields(2))
    val remainingDays = ChronoUnit.DAYS.between(timestamp.toLocalDate(), expiryDate)

    remainingDays match {
      case days if days >= 1 && days <= 29 => 0.01 * (30 - days)
      case _ => 0.0
    }
  }


  // Define a function to check if the product qualifies for a discount
  def qualifyProduct(order: String): Boolean = {
    val fields = order.split(",")
    val productName = fields(1).toLowerCase()
    productName.contains("cheese") || productName.contains("wine")
  }

  // Define a function to calculate the discount based on the product category
  def discountProduct(order: String): Double = {
    if (qualifyProduct(order)) {
      val fields = order.split(",")
      val productName = fields(1).toLowerCase()
      if (productName.contains("cheese"))
        0.1
      else if (productName.contains("wine"))
        0.05
      else
        0.0
    } else {
      0.0
    }
  }


  // Define a function to check if the order was placed on 23rd March
  def qualify23March(order: String): Boolean = {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val fields = order.split(",")
    val timestamp = LocalDateTime.parse(fields(0), dateFormat)
    if (timestamp.getMonthValue == 3 && timestamp.getDayOfMonth == 23) true
    else false
  }


  // Define a function to calculate the discount based on the order placed on 23rd March
  def discount23March(order: String): Double = {
    0.5
  }

  // Define a function to check if the quantity of the order qualifies for a discount
  def qualifyQuantity(order: String): Boolean = {
    val fields = order.split(",")
    if (fields(3).toInt > 5) true
    else false
  }

  // Define a function to calculate the discount based on the quantity of the order
  def discountQuantity(order: String): Double = {
    if (qualifyQuantity(order)) {
      val fields = order.split(",")
      val quantity = fields(3).toInt
      quantity match {
        case q if q >= 6 && q <= 9 => 0.05
        case q if q >= 10 && q <= 14 => 0.07
        case q if q >= 15 => 0.1
        case _ => 0.0
      }
    } else {
      0.0
    }
  }


  def qualifyAppSale(order: String): Boolean = {
    val fields = order.split(",")
    val channel = fields(5)
    channel == "App"
  }

  def discountAppSaleQuantity(order: String): Double = {
    val fields = order.split(",")
    val quantity = fields(3).toInt
    val roundedQuantity = Math.ceil(quantity / 5.0) * 5

    roundedQuantity match {
      case q if q >= 1 && q <= 5 => 0.05
      case q if q >= 6 && q <= 10 => 0.10
      case q if q >= 11 && q <= 15 => 0.15
      case q if q > 15 => 0.20
      case _ => 0.0
    }
  }

  def qualifyVisaSale(order: String): Boolean = {
    val fields = order.split(",")
    val paymentMethod = fields(6)
    paymentMethod == "Visa"
  }

  def discountVisaSale(order: String): Double = {
    if (qualifyVisaSale(order))
      0.05 // 5% discount for Visa card sales
    else
      0.0
  }

  val rules: List[((String) => Boolean, (String) => Double)] = List(
    (qualifyExpiryDate, discountExpiryDate),
    (qualifyProduct , discountProduct),
    (qualifyQuantity , discountQuantity),
    (qualify23March, discount23March),
    (qualifyAppSale , discountAppSaleQuantity),
    (qualifyVisaSale , discountVisaSale)
  )


  def calculateDiscountForOrder(order: String, rules: List[((String) => Boolean, (String) => Double)]): Double = {
    val discounts = rules.flatMap { case (qualify, discount) =>
      if (qualify(order)) Some(discount(order)) else None
    }

    discounts.length match {
      case 0 => 0.0
      case 1 => discounts.head
      case _ =>
        val topTwoDiscounts = discounts.sorted.takeRight(2)
        if (topTwoDiscounts.nonEmpty) topTwoDiscounts.sum / 2 else 0.0
    }
  }


  def calculateFinalPrice(unitPrice: Double, quantity: Int, discount: Double): Double = {
     (unitPrice * quantity) - (unitPrice * quantity * discount)
  }

  val data = orders.map { order =>
    val (timestamp, productName, expiryDate, quantity, unitPrice, channel, paymentMethod) = extractOrderDetails(order)
    val discount = calculateDiscountForOrder(order, rules)
    val finalPrice = calculateFinalPrice(unitPrice, quantity, discount)
    (timestamp.toString, productName, expiryDate.toString, quantity, unitPrice, channel, paymentMethod, discount, finalPrice)
  }
//////////////////////////////////////////////////////// Write to DataBase /////////////////////////////////////////////////////////////////


  def write_to_db(orders: List[String], rules: List[((String) => Boolean, (String) => Double)], writer: PrintWriter, f: File): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val url = "jdbc:oracle:thin:@//localhost:1522/XE"
    val username = "ORDERS"
    val password = "123"



    val insertStatement =
          """
            |INSERT INTO ORDERS (OrderDate, ProductName, ExpiryDate, Quantity, UnitPrice, Channel, PaymentMethod, Discount, Final_Price)
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver") // Load the Oracle JDBC driver
      connection = DriverManager.getConnection(url, username, password)
      log_event(writer, f, "Debug", "Successfully Opened database connection")
      preparedStatement = connection.prepareStatement(insertStatement)


      data.foreach { case (timestamp, productName, expiryDate, quantity, unitPrice, channel, paymentMethod, discount, finalPrice) =>
                preparedStatement.setDate(1, Date.valueOf(timestamp.toString))
                preparedStatement.setString(2, productName)
                preparedStatement.setDate(3, Date.valueOf(expiryDate.toString))
                preparedStatement.setInt(4, quantity)
                preparedStatement.setDouble(5, unitPrice)
                preparedStatement.setString(6, channel)
                preparedStatement.setString(7, paymentMethod)
                preparedStatement.setDouble(8, discount)
                preparedStatement.setDouble(9, finalPrice)


        preparedStatement.addBatch()
      }

      preparedStatement.executeBatch()
      log_event(writer,f , "INFO", "Batch insert executed successfully.")


    } catch {
      case e: Exception =>
        log_event(writer, f, "Error", s"Failed to close preparedStatement: ${e.getMessage}")
    } finally {
      // Close resources
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
      log_event(writer, f, "Info", "Successfully inserted into database")
      log_event(writer, f, "Debug", "Closed database connection")
    }
  }


  val f: File = new File("src/main/Output/logs.log")


  val writer = new PrintWriter(new FileOutputStream(f, true))


  log_event(writer, f, "Info/Debug", "Program Started")


  write_to_db(orders, rules, writer, f)


  log_event(writer, f, "Info/Debug", "Program Finished")


///////////////////////////////////////////////   Write to CSV File ///////////////////////////////////////////////////


  writeToCSV(data, new File("src/main/Output/results.csv"))

  // Function to write data to a CSV file
  def writeToCSV(data: List[(String, String, String, Int, Double, String, String, Double, Double)], file: File): Unit = {
    val writer = new PrintWriter(file)
    try {
      // Write header
      writer.println("Orderdate, Product Name, Expiry Date, Quantity, Unit Price, Channel, Payment Method, Discount, Final Price")

      // Write data rows
      data.foreach { case (timestamp, productName, expiryDate, quantity, unitPrice, channel, paymentMethod, discount, finalPrice) =>
        writer.println(s"$timestamp, $productName, $expiryDate, $quantity, $unitPrice, $channel, $paymentMethod, $discount, $finalPrice")
      }
    } finally {
      writer.close()
    }
  }

}
