package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class SkiierSparkApp {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://skierdbinstance.cmt5itoksgaz.us-west-2.rds.amazonaws.com/skierdb";
        SparkSession spark = SparkSession.builder()
                .appName("Simple Application")
                .getOrCreate();
        Dataset<Row> rides = spark
                .read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "rides")
                .option("driver", "org.postgresql.Driver")
                .option("user", "tinavivio")
                .option("password", "rahul2016")
                .load();
        Dataset<Row> lifts = spark
                .read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "lifts")
                .option("driver", "org.postgresql.Driver")
                .option("user", "tinavivio")
                .option("password", "rahul2016")
                .load();
        
        Dataset<Row> countsByLiftNumberAndDayNumber = rides.groupBy("liftNumber", "dayNumber").count();
        Dataset<Row> mostPopularLiftByDay = countsByLiftNumberAndDayNumber.groupBy("dayNumber").agg(org.apache.spark.sql.functions.max("count"));
        Dataset<Row> countsBySkierIdAndDayNumberAndLiftNumber = rides.groupBy("skierId", "dayNumber", "liftNumber").count();
        Dataset<Row> totalHeightBySkierIdAndDayNumberAndLiftNumber = countsBySkierIdAndDayNumberAndLiftNumber
                .join(lifts)
                .where(countsBySkierIdAndDayNumberAndLiftNumber.col("liftNumber").equalTo(lifts.col("liftNumber")))
                .select(countsBySkierIdAndDayNumberAndLiftNumber.col("skierId"), 
                        countsBySkierIdAndDayNumberAndLiftNumber.col("dayNumber"), 
                        countsBySkierIdAndDayNumberAndLiftNumber.col("count").multiply(lifts.col("height")).alias("totalHeightForDayAndLift"));
        Dataset<Row> totalHeightBySkierIdAndDayNumber = totalHeightBySkierIdAndDayNumberAndLiftNumber.groupBy("skierId", "dayNumber").agg(org.apache.spark.sql.functions.sum("totalHeightForDayAndLift").alias("totalHeightForDay"));
        Dataset<Row> mostProlificSkierByDay = totalHeightBySkierIdAndDayNumber.groupBy("dayNumber").agg(org.apache.spark.sql.functions.max("totalHeightForDay"));
        
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "tinavivio");
        connectionProperties.put("password", "rahul2016");
        connectionProperties.put("driver", "org.postgresql.Driver");
        
        countsByLiftNumberAndDayNumber.coalesce(1).write().jdbc(url, "countsByLiftNumberAndDayNumber", connectionProperties);
        mostPopularLiftByDay.coalesce(1).write().jdbc(url, "mostPopularLiftByDay", connectionProperties);
        countsBySkierIdAndDayNumberAndLiftNumber.coalesce(1).write().jdbc(url, "countsBySkierIdAndDayNumberAndLiftNumber", connectionProperties);
        totalHeightBySkierIdAndDayNumber.coalesce(1).write().jdbc(url, "totalHeightBySkierIdAndDayNumber", connectionProperties);
        mostProlificSkierByDay.coalesce(1).write().jdbc(url, "mostProlificSkierByDay", connectionProperties);
        
        spark.stop();
    }
}
