import logic_use_cases.CustomersOrders;
import main_use_cases.ReadData;
import main_use_cases.Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import static org.junit.Assert.assertEquals;

public class CustomerOrdersTest extends Util {
    private static Dataset<Row> orderData;
    private static Dataset<Row> customerData;
    private static Map<String, String> env = System.getenv();
    private static String readDir = "C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\use-case-retail_db\\retail_db\\read";
    private static String writeDir = "C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\use-case-retail_db\\retail_db\\write";
    private static CustomersOrders csObj=new CustomersOrders();
    private static ReadData dfObj=new ReadData();
    private static Dataset<Row> resultOrdersCount;
    private static Dataset<Row> resultDormant;
    @BeforeClass
    public static void beforeClass(){
        orderData=dfObj.retailDbData(readDir,"orders");
        customerData=dfObj.retailDbData(readDir,"customers");
        csObj.custOrdersCount(orderData,customerData,writeDir);
        csObj.dormantCustomers(orderData,customerData,writeDir);
        resultOrdersCount=csObj.resultOrdersCount;
        resultDormant=csObj.resultDormant;
    }

    //test case for orders count per customer
    @Test
    public void testOrdersCountCustomer(){
        long expectedOrderCount=orderData.filter("order_date like '2014-01%' and order_customer_id=8622").count();
        long actualOrdersCount=resultOrdersCount.select("customer_order_count").collectAsList().get(0).getLong(0);
        System.out.println(expectedOrderCount);
        System.out.println(actualOrdersCount);
        assertEquals(expectedOrderCount,actualOrdersCount);
    }

    //test case for dormant customers
    @Test
    public void testDormantCustomers(){
        long actualDormantCustomers=resultDormant.count();
        int expectedDormantCustomers=7739;
        assertEquals(expectedDormantCustomers,actualDormantCustomers);
    }
}
