import logic_use_cases.RevenueGenerator;
import main_use_cases.ReadData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.apache.spark.sql.functions.col;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RevenueGeneratorTest {
    private static Dataset<Row> orderData;
    private static Dataset<Row> prodData;
    private static Dataset<Row> ordItemData;
    private static Dataset<Row> categoryData;
    private static Dataset<Row> customerData;
    private static Map<String, String> env = System.getenv();
    private static String readDir = "C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\use-case-retail_db\\retail_db\\read";
    private static String writeDir = "C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\use-case-retail_db\\retail_db\\write";
    private static ReadData dfObj=new ReadData();
    private static RevenueGenerator rgObj=new RevenueGenerator();
    private static Dataset<Row> resultRevenueCategory;
    private static Dataset<Row> resultRevenuePerCust;
    @BeforeClass
    public static void beforeClass(){
        orderData=dfObj.retailDbData(readDir,"orders");
        prodData=dfObj.retailDbData(readDir,"products");
        categoryData=dfObj.retailDbData(readDir,"categories");
        ordItemData=dfObj.retailDbData(readDir,"order_items");
        customerData=dfObj.retailDbData(readDir,"customers");
        rgObj.revenuePerCategory(orderData,categoryData,ordItemData,prodData,writeDir);
        resultRevenueCategory=rgObj.resultRevenueCategory;
        rgObj.revenuePerCust(orderData,customerData,ordItemData,writeDir);
        resultRevenuePerCust=rgObj.resultRevenueCust;

    }

    //test case to check actual and expected count of category_id in 2014 Jan having order_status ('COMPLETE','CLOSED')
    @Test
    public void testRevenuePerCategory(){
        long actualRevenueCount=resultRevenueCategory.count();
        long expectedRevenueCount=prodData.join(ordItemData,ordItemData.col("order_item_product_id")
                        .equalTo(prodData.col("product_id")))
                .join(orderData, orderData.col("order_id").equalTo(ordItemData.col("order_item_order_id")))
                .filter("order_date like '2014-01%' AND order_status in ('COMPLETE','CLOSED')")
                .select(col("product_category_id")).distinct().count();
        assertEquals(expectedRevenueCount,actualRevenueCount);
    }

    //test case for revenue per customer
    @Test
    public void testRevenuePerCust(){
        Dataset<Row> new_ord_df=orderData.filter("order_date like '2014-01%' AND order_status in ('COMPLETE','CLOSED')");
        long expectedRevenueCount=new_ord_df.join(customerData,new_ord_df.col("order_customer_id")
                .equalTo(customerData.col("customer_id")),"right_outer")
                .select("customer_id").distinct().count();
        long actualRevenueCount=resultRevenuePerCust.count();
        assertEquals(expectedRevenueCount,actualRevenueCount);
    }
}
