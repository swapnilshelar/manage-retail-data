package main_use_cases;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ReadData extends Util{
    //method to read data from file which is at specified location
    public Dataset<Row> retailDbData(String mainDir, String name){
        return spark.read().option("header","true").csv(mainDir+"//"+name);
    }
}
