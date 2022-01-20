package logic_use_cases;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class CustomersOrders {
    public CustomersOrders() {
        PropertyConfigurator.configure("src/main/resources/log4j_use_case.properties");
    }
    private String customerId="customer_id";
    public Dataset<Row> resultOrdersCount;
    public Dataset<Row> resultDormant;
    private static Logger logger = Logger.getLogger("CustomersOrders");

    /*    Usecase 1 - Customer order count
          Get order count per customer for the month of 2014 January.
          * Tables - orders and customers
          * Data should be sorted in descending order by count and ascending order by customer id.
          * Output should contain customer_id, customer_first_name, customer_last_name and customer_order_count.
    */
    public void custOrdersCount(Dataset<Row> orderData, Dataset<Row> customerData, String writeDir) {

        resultOrdersCount = orderData.join(customerData, orderData.col("order_customer_id")
                        .equalTo(customerData.col(customerId)), "inner")
                .filter("order_date like '2014-01%'")
                .groupBy(customerId, "customer_fname", "customer_lname")
                .agg(count("order_id").as("customer_order_count"))
                .orderBy(col("customer_order_count").desc(), col(customerId).cast("int"));
        resultOrdersCount.show();
        logger.info("Result - orders count per customer is ready");
        resultOrdersCount.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(writeDir + "\\" + "customers_orders_count");
        logger.info("Result is written into specific location...");
    }

    /*   Usecase 2 - Dormant Customers
         Get the customer details who have not placed any order for the month of 2014 January.
         * Tables - orders and customers
         * Data should be sorted in ascending order by customer_id
         * Output should contain all the fields from customers
    */
    public void dormantCustomers(Dataset<Row> orderData, Dataset<Row> customerData, String writeDir) {

        Dataset<Row> newOrdersData = orderData.filter("order_date like '2014-01%'");
        resultDormant = newOrdersData.join(customerData, orderData.col("order_customer_id")
                        .equalTo(customerData.col(customerId)), "right_outer")
                .filter("order_date is null")
                .orderBy(col("customer_id").cast("int"))
                .select(customerId, "customer_fname", "customer_lname", "customer_email",
                        "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode");
        resultDormant.show();
        logger.info("Result - dormant customers ");
        resultDormant.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(writeDir + "\\" + "dormant_customers");
        logger.info("Result is written into specific location...");
    }
}

