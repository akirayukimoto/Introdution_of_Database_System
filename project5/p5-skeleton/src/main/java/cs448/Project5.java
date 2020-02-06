package cs448;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

public class Project5 {
    public void runSparkApp1(App.Conf conf){
        System.out.println("Running Your First Spark App!");
        /* Hint: @see App#warmupExercise() for a sample Spark application code
        * using SparkRDD API
        */

        // Create a Spark Session.
        SparkSession sparkSession = SparkSession.builder().appName("CS448 Project 5 -- Query 1").getOrCreate();

        // Write data processing code here
        // Using Spark SQL
        JavaRDD<User> userJavaRDD = sparkSession.read().textFile(
                CS448Utils.resolveUri(conf.inPath, conf.usersFName))
                .cache().javaRDD().map(User::parseUser);

        JavaRDD<Movie> movieJavaRDD = sparkSession.read().textFile(
                CS448Utils.resolveUri(conf.inPath, conf.moviesFName))
                .cache().javaRDD().map(Movie::parseMovie);

        JavaRDD<Rating> ratingJavaRDD = sparkSession.read().textFile(
                CS448Utils.resolveUri(conf.inPath, conf.ratingsFName))
                .cache().javaRDD().map(Rating::parseRating);

        Dataset<Row> userDataset = sparkSession.createDataFrame(userJavaRDD, User.class);
        Dataset<Row> movieDataset = sparkSession.createDataFrame(movieJavaRDD, Movie.class);
        Dataset<Row> ratingDataset = sparkSession.createDataFrame(ratingJavaRDD, Rating.class);

        userDataset.createOrReplaceTempView("users");
        movieDataset.createOrReplaceTempView("movies");
        ratingDataset.createOrReplaceTempView("ratings");

        String query =
                "select distinct t.title " +
                        "from (" +
                        "select title, rating, occupation " +
                        "from movies m " +
                        "join ratings r on m.movieid=r.movieid " +
                        "join users u on u.userid=r.userid) t " +
                        "where t.rating>=" + conf.q1Rating + " " +
                        "and t.occupation=" + conf.q1Occupation;


        Dataset<Row> result = sparkSession.sql(query);
//        result.show();
//        String outputFPath = CS448Utils.resolveUri(conf.outPath,"query-1");
//        result.write().text(outputFPath);
        result.javaRDD().map(r -> r.getString(0))
                .saveAsTextFile(CS448Utils.resolveUri(conf.outPath,"query-1"));
        sparkSession.stop();
    }

    public void runSparkApp2(App.Conf conf){
        System.out.println("Running Spark App for Query 2");
        // Create a Spark Session.
    }

    public void runSparkApp3(App.Conf conf){
        System.out.println("Running Spark App for Query 3");
        // Write your code here
    }

    public void runSparkApp4(App.Conf conf){
        System.out.println("Running Spark App for Query 4");
        // Write your code here

        // Create a Spark Session.
        SparkSession sparkSession = SparkSession.builder().appName("CS448 Project 5 -- Query 4").getOrCreate();

        // Write data processing code here
        // Using Spark SQL
        JavaRDD<User> userJavaRDD = sparkSession.read().textFile(
                CS448Utils.resolveUri(conf.inPath, conf.usersFName))
                .cache().javaRDD().map(User::parseUser);

        JavaRDD<Movie> movieJavaRDD = sparkSession.read().textFile(
                CS448Utils.resolveUri(conf.inPath, conf.moviesFName))
                .cache().javaRDD().map(Movie::parseMovie);

        JavaRDD<Rating> ratingJavaRDD = sparkSession.read().textFile(
                CS448Utils.resolveUri(conf.inPath, conf.ratingsFName))
                .cache().javaRDD().map(Rating::parseRating);

        Dataset<Row> userDataset = sparkSession.createDataFrame(userJavaRDD, User.class);
        Dataset<Row> movieDataset = sparkSession.createDataFrame(movieJavaRDD, Movie.class);
        Dataset<Row> ratingDataset = sparkSession.createDataFrame(ratingJavaRDD, Rating.class);

        userDataset.createOrReplaceTempView("users");
        movieDataset.createOrReplaceTempView("movies");
        ratingDataset.createOrReplaceTempView("ratings");

        String query = "select m.title, count(r.rating) as ratingcount " +
                "from movies as m join ratings as r on m.movieid=r.movieid, users as u " +
                "where u.age=" + conf.q4Age + " and u.userid=r.userid " +
                "group by m.title";

        Dataset<Row> result = sparkSession.sql(query);
        result.show();
        String outputFPath = CS448Utils.resolveUri(conf.outPath, "/query-4");
        result.javaRDD().map(row -> row.mkString("::")).saveAsTextFile(outputFPath);
        sparkSession.stop();
    }
}
