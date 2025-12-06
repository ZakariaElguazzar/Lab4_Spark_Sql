import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BikeSharingAppHdfs {

    public static void main(String[] args) {

        // I. Initialize Spark Session

        // Configuration avec HDFS
        SparkConf conf = new SparkConf()
                .setAppName("BikeSharingAnalysisHDFS")
                .set("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
                .set("spark.hadoop.dfs.replication", "1");

        String master = args.length > 0 ? args[0] : "spark://spark-master:7077";
        conf.setMaster(master);


        SparkSession spark = SparkSession.builder()
                .config(conf)// change to yarn/cluster master if needed
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // II. Load CSV from HDFS
        String hdfsPath = "hdfs://namenode:8020/bike_sharing.csv";

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(hdfsPath);

        // III. Data Exploration
        df.printSchema();
        df.show(5);
        System.out.println("Number of rentals: " + df.count());

        // IV. Create Temporary View
        df.createOrReplaceTempView("bike_rentals_view");

        System.out.println("Temporary view 'bike_rentals_view' created successfully.");

        // V. Queries (Examples)
        // 1. Rentals longer than 30 minutes
        Dataset<Row> longRentals = spark.sql(
                "SELECT * FROM bike_rentals_view WHERE duration_minutes > 30"
        );
        longRentals.show((int) longRentals.count(), false);

        // 2. Rentals starting at "Station A"
        Dataset<Row> stationARentals = spark.sql(
                "SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'"
        );
        stationARentals.show(false);

        // 3. Total revenue
        Dataset<Row> totalRevenue = spark.sql(
                "SELECT SUM(price) AS total_revenue FROM bike_rentals_view"
        );
        totalRevenue.show(false);

        // VI. Aggregation Queries
        Dataset<Row> rentalsPerStation = spark.sql(
                "SELECT start_station, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view GROUP BY start_station"
        );
        rentalsPerStation.show(false);

        Dataset<Row> avgDurationPerStation = spark.sql(
                "SELECT start_station, AVG(duration_minutes) AS avg_duration " +
                        "FROM bike_rentals_view GROUP BY start_station"
        );
        avgDurationPerStation.show(false);

        Dataset<Row> topStation = spark.sql(
                "SELECT start_station, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view GROUP BY start_station " +
                        "ORDER BY rental_count DESC LIMIT 1"
        );
        topStation.show(false);

        // VII. Example: Write output back to HDFS (optional)
        // Example: Save rentalsPerStation as CSV
        rentalsPerStation.write()
                .option("header", "true")
                .mode("overwrite")
                .csv("hdfs://namenode:8020/rentals_per_station");

        // Stop Spark session
        spark.stop();
    }
}
