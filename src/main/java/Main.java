import by.sanko.spark.two.entity.HotelData;
import by.sanko.spark.two.parser.HotelParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;

public class Main {
    private static HashMap<Long, HotelData> hotelData = new HashMap<>();
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> data2016 = spark.read().format("csv")
                .option("header", "true")
                .load("/user/hadoop/task1/expedia/new_ver/year=2016/*.csv");
        String[] strings = data2016.columns();
        System.out.println("Expedia rows are " + data2016.count());
        System.out.println("Schema is " + data2016.schema());
        for(String part : strings){
            System.out.println("Part is     " + part);
        }
        data2016.show();
    }

    private static void invokeHotelData(){
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host.docker.internal:9094")
                .option("subscribe", "hw-data-topic") //weathers-data-hash
                .load();
        spark.sparkContext().setLogLevel("ERROR");
        List<String> stringList = df.selectExpr("CAST(value AS STRING)").as(Encoders.STRING()).collectAsList();
        List<String> hotels = new ArrayList<>();
        for(String value : stringList){
            int index = value.indexOf('\n');
            String tmp = value.substring(index + 1, value.indexOf('\n', index +1));
            hotels.add(tmp);
        }
        for(String hotel : hotels){
            HotelData data = HotelParser.parseData(hotel);
            hotelData.put(data.getId(), data);
        }
        System.out.println("Hotel data is " + hotelData.size());
        long numAs = df.count();
        System.out.println("Lines at all: " + numAs);
    }
}
