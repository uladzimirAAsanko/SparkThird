import by.sanko.spark.two.entity.HotelData;
import by.sanko.spark.two.entity.StayType;
import by.sanko.spark.two.parser.HotelParser;
import by.sanko.spark.two.parser.Parser;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class Main {
    private static final HashMap<Long, HotelData> hotelData = new HashMap<>();
    private static final HashMap<Long, HashMap<String, Double>> hotelWeatherHM = new HashMap<>();

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        invokeHotelData();
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
        String HOTEL_WEATHER_JOINED = "hotel-and-weather-joined-simple";
        readWthData(spark, HOTEL_WEATHER_JOINED);
        List<Row> correctSet = new ArrayList<>();
        List<Row> list = data2016
                .selectExpr("CAST(hotel_id AS LONG)", "CAST(srch_ci AS STRING)", "CAST(srch_co AS STRING)",
                        "CAST(id AS LONG)","CAST(srch_children_cnt AS INT)")
                .collectAsList();
        for(Row row : list){
            Long hotelID = row.getLong(0);
            String checkIN = row.getString(1);
            String checkOUT = row.getString(2);
            Long id = row.getLong(3);
            Integer withChld  = row.getInt(4);
            HashMap<String, Double> map = hotelWeatherHM.get(hotelID);
            if(map != null && map.get(checkIN) != null && map.get(checkIN) > 0){
                int stayType = StayType.calculateType(checkIN, checkOUT).getStayID();
                correctSet.add(RowFactory.create(id, hotelID, checkIN, checkOUT, map.get(checkIN), stayType,withChld));
            }
        }

        List<org.apache.spark.sql.types.StructField> listOfStructField = new ArrayList<>();
        listOfStructField.add(DataTypes.createStructField("id",DataTypes.LongType,false));
        listOfStructField.add(DataTypes.createStructField("hotel_id",DataTypes.LongType,false));
        listOfStructField.add(DataTypes.createStructField("checkIn",DataTypes.StringType,false));
        listOfStructField.add(DataTypes.createStructField("checkOut",DataTypes.StringType,false));
        listOfStructField.add(DataTypes.createStructField("avg_tmp",DataTypes.DoubleType,false));
        listOfStructField.add(DataTypes.createStructField("stay_type",DataTypes.IntegerType,false));
        listOfStructField.add(DataTypes.createStructField("cnt_child",DataTypes.IntegerType,false));
        StructType structType = DataTypes.createStructType(listOfStructField);
        Dataset<Row> cleanedAndMarkedDataset = spark.createDataFrame(correctSet, structType);



        System.out.println("Delete all invalid data and check stay_type ");
        cleanedAndMarkedDataset.show();
        List<Long> listOfHotels = cleanedAndMarkedDataset
                .selectExpr("CAST(hotel_id AS LONG)")
                .distinct()
                .as(Encoders.LONG())
                .collectAsList();
        System.out.println("Start to counting by typed of data ");
        System.out.println("Uniq hotels in cleaned dataset " + listOfHotels.size());
        List<Row> answerData = new ArrayList<>();
        int i = 0;
        for(Long hotelID : listOfHotels){
            i++;
            System.out.println("Processing rows of hotel with id " + hotelID + " hotel is " + i);
            Dataset<Row> allRowsWithHotel = cleanedAndMarkedDataset.where("hotel_id="+hotelID);
            long allCount = allRowsWithHotel.count();
            long countChild = allRowsWithHotel.where("cnt_child>0").count();
            long shortStayCount = allRowsWithHotel
                    .where("stay_type="+StayType.SHORT_STAY.getStayID()).count();
            long max = shortStayCount;
            StayType mostPopular = StayType.SHORT_STAY;
            long erroneousCount = allRowsWithHotel
                    .where("stay_type="+StayType.ERRONEOUS_DATA.getStayID()).count();
            if(erroneousCount > max){
                max = erroneousCount;
                mostPopular = StayType.ERRONEOUS_DATA;
            }
            long standStayCount = allRowsWithHotel
                    .where("stay_type="+StayType.STANDARD_STAY.getStayID()).count();
            if(standStayCount > max){
                max = standStayCount;
                mostPopular = StayType.STANDARD_STAY;
            }
            long standExtendStayCount = allRowsWithHotel
                    .where("stay_type="+StayType.STANDARD_EXTENDED_STAY.getStayID()).count();
            if(standExtendStayCount > max){
                max = standExtendStayCount;
                mostPopular = StayType.STANDARD_EXTENDED_STAY;
            }
            long longStayCount = allRowsWithHotel
                    .where("stay_type="+StayType.LONG_STAY.getStayID()).count();
            if(longStayCount > max){
                mostPopular = StayType.LONG_STAY;
            }
            String name = hotelData.get(hotelID).getName();
            answerData.add(RowFactory.create(hotelID, name,allCount, countChild, shortStayCount,erroneousCount,
                    standStayCount, standExtendStayCount, longStayCount, mostPopular.toString()));
        }

        List<org.apache.spark.sql.types.StructField> structs = new ArrayList<>();
        structs.add(DataTypes.createStructField("hotel_id",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("hotel_name",DataTypes.StringType,false));
        structs.add(DataTypes.createStructField("all_cnt",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("child_cnt",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("short_stay_cnt",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("erroneous_stay_cnt",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("stand_stay_cnt",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("stand_extended_stay_cnt",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("long_stay_cnt",DataTypes.LongType,false));
        structs.add(DataTypes.createStructField("most_popular",DataTypes.StringType,false));
        StructType structures = DataTypes.createStructType(structs);
        Dataset<Row> answerAtAll = spark.createDataFrame(answerData, structures);
        answerAtAll.show();
        System.out.println("Temp size is " + correctSet.size());
        System.out.println("Write 2016 data into csv file into folder");
        answerAtAll.write().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .save("/user/hadoop/task1/expedia/spark-save/2016/");
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
        hotelWeatherHM.forEach((k,v)-> i.addAndGet(v.size()));
        System.out.println("All values are " + i);
    }
}
