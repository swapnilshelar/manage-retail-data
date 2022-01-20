package main_use_cases;
import org.apache.spark.sql.SparkSession;
public class Util {
    public static final SparkSession spark = SparkSession
            .builder()
            .master("local")
            .appName("Java Spark SQL basic example")
            .getOrCreate();

}
