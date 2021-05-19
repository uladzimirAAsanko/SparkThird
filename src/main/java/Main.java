import by.sanko.spark.two.entity.HotelData;
import by.sanko.spark.two.entity.StayType;
import by.sanko.spark.two.parser.HotelParser;
import by.sanko.spark.two.parser.Parser;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.desc;

public class Main {
    private static HashMap<Long, HotelData> hotelData = new HashMap<>();
    private static String HOTEL_WEATHER_JOINED = "hotel-and-weather-joined-simple";
    private static HashMap<Long, HashMap<String, Double>> hotelWeatherHM = new HashMap<>();
    private static char comma = ',';

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> data2016 = spark.read().format("csv")
                .option("header", "true")
                .option("delimiter", ";")
                .load("/user/hadoop/task1/expedia/new_ver/year=2016/*.csv");
        String[] strings = data2016.columns();
        System.out.println("Expedia rows are " + data2016.count());
        System.out.println("Schema is " + data2016.schema());
        for(String part : strings){
            System.out.println("Part is     " + part);
        }
        readWthData(spark, HOTEL_WEATHER_JOINED);
        List<Row> correctSet = new ArrayList<Row>();
        List<Row> list = data2016
                .selectExpr("CAST(hotel_id AS LONG)", "CAST(srch_ci AS STRING)", "CAST(srch_co AS STRING)", "CAST(id AS LONG)")
                .collectAsList();
        for(Row row : list){
            Long hotelID = row.getLong(0);
            String checkIN = row.getString(1);
            String checkOUT = row.getString(2);
            Long id = row.getLong(3);
            HashMap<String, Double> map = hotelWeatherHM.get(hotelID);
            if(map != null && map.get(checkIN) != null && map.get(checkIN) > 0){
                int stayType = StayType.calculateType(checkIN, checkOUT).getStayID();
                correctSet.add(RowFactory.create(id, hotelID, checkIN, checkOUT, map.get(checkIN), stayType));
            }
        }
        List<org.apache.spark.sql.types.StructField> listOfStructField=
                new ArrayList<org.apache.spark.sql.types.StructField>();
        listOfStructField.add(DataTypes.createStructField("id",DataTypes.LongType,false));
        listOfStructField.add(DataTypes.createStructField("hotel_id",DataTypes.LongType,false));
        listOfStructField.add(DataTypes.createStructField("checkIn",DataTypes.StringType,false));
        listOfStructField.add(DataTypes.createStructField("checkOut",DataTypes.StringType,false));
        listOfStructField.add(DataTypes.createStructField("avg_tmp",DataTypes.DoubleType,false));
        listOfStructField.add(DataTypes.createStructField("stay_type",DataTypes.IntegerType,false));
        StructType structType = DataTypes.createStructType(listOfStructField);
        Dataset<Row> dataset = spark.createDataFrame(correctSet, structType);
        dataset.where("stay_type=1").groupBy("hotel_id").count().orderBy(desc("count()")).show();
        System.out.println("Temp size is " + correctSet.size());
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

    private static void readWthData(SparkSession spark, String topicName){
        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host.docker.internal:9094")
                .option("subscribe", topicName) //weathers-data-hash
                .load();
        df.selectExpr("CAST(value AS STRING)").show();
        String[] strings = df.columns();
        for(String part : strings){
            System.out.println("Part is     " + part);
        }
        df.selectExpr("CAST(value AS STRING)").foreach(row -> {
            String value = row.getString(0);
            int indexOfComma = value.indexOf(Parser.comma);
            Long hotelID = Long.parseLong(value.substring(0,indexOfComma));
            indexOfComma ++;
            int indexOfNextComma = value.indexOf(Parser.comma, indexOfComma);
            String date = value.substring(indexOfComma, indexOfNextComma);
            Double avg = Double.parseDouble(value.substring(indexOfNextComma+1));
            HashMap<String, Double> map = hotelWeatherHM.get(hotelID);
            if(map == null){
                map = new HashMap<>();
                map.put(date,avg);
                hotelWeatherHM.put(hotelID, map);
            }else{
                map.put(date,avg);
            }
        });
        System.out.println("Hotel key size is " + hotelWeatherHM.keySet().size());
        AtomicInteger i = new AtomicInteger();
        hotelWeatherHM.forEach((k,v)->{
            i.addAndGet(v.size());
        });
        System.out.println("All values are " + i);
    }
}
