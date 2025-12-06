import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BikeSharingAppLocal {

    public static void main(String[] args) {

        // I. Data Loading & Exploration

        SparkSession spark = SparkSession.builder()
                .appName("BikeSharingAnalysis")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");


        // 1. Load CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("bike_sharing.csv");

        // 2. Display schema
        df.printSchema();

        // 3. Show first 5 rows
        df.show(5);

        // 4. Count number of rentals
        long count = df.count();
        System.out.println("Number of rentals: " + count);

        // II. Create a Temporary View

        // 1. Create temporary SQL view (You turn df into a SQL Table,You can query this view using Spark SQL)
        df.createOrReplaceTempView("bike_rentals_view");

        System.out.println("Temporary view 'bike_rentals_view' created successfully.");

        // III. Basic SQL Queries

        // 1. List all rentals longer than 30 minutes.

        Dataset<Row> longRentals = spark.sql(
                "SELECT * " +
                        "FROM bike_rentals_view " +
                        "WHERE duration_minutes > 30"
        );

        System.out.println("All rentals longer than 30 minutes.");

        longRentals.show((int) longRentals.count(), false);

        // 2. Show all rentals starting at "Station A".

        System.out.println("All rentals starting at Station A.");

        Dataset<Row> stationARentals = spark.sql(
                "SELECT * " +
                        "FROM bike_rentals_view " +
                        "WHERE start_station = 'Station A'"
        );
        stationARentals.show();

        // 3. Calculate the total revenue (sum of the column price).

        System.out.println("Total Revenue.");

        Dataset<Row> totalRevenue = spark.sql(
                "SELECT SUM(price) AS total_revenue " +
                        "FROM bike_rentals_view"
        );
        totalRevenue.show();

        // IV. Aggregation Queries

        // 1. Count how many rentals were made from each start station.
        System.out.println("Number of rentals from each start station.");

        Dataset<Row> rentalsPerStation = spark.sql(
                "SELECT start_station, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station"
        );

        rentalsPerStation.show();


        // 2. Compute the average rental duration per start station.
        System.out.println("Average rental duration per start station.");

        Dataset<Row> avgDurationPerStation = spark.sql(
                "SELECT start_station, AVG(duration_minutes) AS avg_duration " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station"
        );

        avgDurationPerStation.show();

        // 3. Identify the station with the highest number of rentals.
        System.out.println("Station with the highest number of rentals.");

        Dataset<Row> topStation = spark.sql(
                "SELECT start_station, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY rental_count DESC " +
                        "LIMIT 1"
        );

        topStation.show();

        topStation.show();

        // V. Time-Based Analysis

        // 1. Extract the hour from start_time.
        System.out.println("Extracting hour from start_time.");
        Dataset<Row> rentalsWithHour = spark.sql(
                "SELECT *, HOUR(start_time) AS rental_hour " +
                        "FROM bike_rentals_view"
        );

        rentalsWithHour.show();

        // 2. Count how many bikes were rented per hour (identify peak hours).
        System.out.println("Number of bikes rented per hour.");

        Dataset<Row> rentalsPerHour = spark.sql(
                "SELECT HOUR(start_time) AS rental_hour, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY rental_hour " +
                        "ORDER BY rental_hour"
        );

        rentalsPerHour.show();

        // 3. Determine the most popular start station during the morning (7–12).
        System.out.println("Most popular start station during the morning (7–12).");
        Dataset<Row> popularMorningStation = spark.sql(
                "SELECT start_station, COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view " +
                        "WHERE HOUR(start_time) BETWEEN 7 AND 12 " +
                        "GROUP BY start_station " +
                        "ORDER BY rental_count DESC " +
                        "LIMIT 1"
        );

        popularMorningStation.show();


        // VI. User Behavior Analysis
        // 1. Compute the average age of users.
        System.out.println("Average age of users.");
        Dataset<Row> avgUserAge = spark.sql(
                "SELECT AVG(age) AS average_age " +
                        "FROM bike_rentals_view"
        );
        avgUserAge.show();
        // 2. Count users by gender.
        System.out.println("Count of users by gender.");
        Dataset<Row> usersByGender = spark.sql(
                "SELECT gender, COUNT(*) AS user_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY gender"
        );
        usersByGender.show();

        // 3. Find which age group rents bicycles the most: 18–30, 31–40, 41–50, 51+.
        System.out.println("Age group that rents bicycles the most.");
        Dataset<Row> ageGroupRentals = spark.sql(
                "SELECT " +
                        "CASE " +
                        "WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                        "WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                        "WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                        "WHEN age >= 51 THEN '51+' " +
                        "END AS age_group, " +
                        "COUNT(*) AS rental_count " +
                        "FROM bike_rentals_view " +
                        "GROUP BY age_group " +
                        "ORDER BY rental_count DESC " +
                        "LIMIT 1"
        );

        ageGroupRentals.show();


        // Stop Spark session
        spark.stop();




    }

}
