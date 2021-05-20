package by.sanko.spark.two.parser;

import by.sanko.spark.two.entity.HotelData;

import java.util.List;

public class HotelParser {
    private static final char comma = ',';

    public static HotelData parseData(String data){
        if(data == null || data.isEmpty()){
            return null;
        }
        List<String> list = Parser.parse(data, 8);
        long id = Long.parseLong(list.get(0));
        String name = list.get(1);
        String country = list.get(2);
        String city = list.get(3);
        String address = list.get(4);
        double lng;
        String geoHash = list.get(7);
        double lat;
        try {
            lat = Double.parseDouble(list.get(6));
            lng = Double.parseDouble(list.get(5));
        }catch (NumberFormatException e){
            geoHash = data.substring(data.lastIndexOf(comma) + 1);
            data = data.substring(0, data.lastIndexOf(comma));
            lat = Double.parseDouble(data.substring(data.lastIndexOf(comma) + 1));
            data = data.substring(0, data.lastIndexOf(comma));
            lng = Double.parseDouble(data.substring(data.lastIndexOf(comma) + 1));
            data = data.substring(0, data.lastIndexOf(comma));
            address = address + data.substring(data.lastIndexOf(comma) + 1);
        }
        return new HotelData(id, name, country, city ,address, lng, lat, geoHash);
    }
}