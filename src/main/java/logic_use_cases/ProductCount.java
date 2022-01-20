package logic_use_cases;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
public class ProductCount {
    public ProductCount(){
        PropertyConfigurator.configure("src/main/resources/log4j_use_case.properties");
    }
    private String departmentId="department_id";
    private static Logger logger=Logger.getLogger("ProductCount");
    public Dataset<Row> resultprodCount;

    /*
          Usecase 5 - Product Count Per Department
          Get the products for each department.
          * Tables - departments, categories, products
          * Data should be sorted in ascending order by department_id
          * Output should contain all the fields from department and the product count as product_count
    */
    public void productCountPerDept(Dataset<Row> deptData,Dataset<Row> categoryData, Dataset<Row> prodData,String writeDir){
        resultprodCount=prodData.join(categoryData,prodData.col("product_category_id")
                        .equalTo(categoryData.col("category_id")),"inner")
                .join(deptData,deptData.col(departmentId)
                        .equalTo(categoryData.col("category_department_id")),"inner")
                .groupBy(deptData.col(departmentId),deptData.col("department_name"))
                .count().alias("product_count")
                .orderBy(deptData.col(departmentId));

        logger.info("Result of product count per category is ready...");
        resultprodCount.show();

        resultprodCount.coalesce(1).write().option("header", "true").mode("overwrite")
                .csv(writeDir + "\\" +"productCountPerDept");
        logger.info("Result is written into specific location...");
    }
}
