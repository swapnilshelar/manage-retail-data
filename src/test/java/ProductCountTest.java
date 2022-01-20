import logic_use_cases.ProductCount;
import main_use_cases.ReadData;
import main_use_cases.Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ProductCountTest extends Util {
    private static Dataset<Row> prodData;
    private static Dataset<Row> categoryData;
    private static Dataset<Row> deptData;
    private static Map<String, String> env = System.getenv();
    private static String readDir = "C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\use-case-retail_db\\retail_db\\read";
    private static String writeDir = "C:\\Users\\Swapnil Shelar\\IdeaProjects\\spark-java\\use-case-retail_db\\retail_db\\write";
    private static ProductCount pcObj=new ProductCount();
    private static ReadData dfObj=new ReadData();
    private static Dataset<Row> resultProdCount;
    @BeforeClass
    public static void beforeClass(){
        prodData=dfObj.retailDbData(readDir,"products");
        categoryData=dfObj.retailDbData(readDir,"categories");
        deptData=dfObj.retailDbData(readDir,"departments");
        pcObj.productCountPerDept(deptData,categoryData,prodData,writeDir);
        resultProdCount = pcObj.resultprodCount;
    }

    //Test Case to check Number Of Departments are same in department dataframe and output department count -groupBy clause
    @Test
    public void DeptCountTest(){
        long actualDeptCount=resultProdCount.select("department_id").count();
        long expectedDeptCount=deptData.select("department_id").count();
        assertEquals(actualDeptCount,expectedDeptCount);
    }

    //Test case to check actual product count per department and product count we get.
    @Test
    public void FitnessDeptCount(){
        long actualCount = resultProdCount.select("count").collectAsList().get(0).getLong(0);
        long expectedCount=prodData.join(categoryData,prodData.col("product_category_id")
                .equalTo(categoryData.col("category_id")),"inner")
                .filter("category_department_id=2").select("product_id").count();
        assertEquals(actualCount,expectedCount);
    }
}
