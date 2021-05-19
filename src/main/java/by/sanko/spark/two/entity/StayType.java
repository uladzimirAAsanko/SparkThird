package by.sanko.spark.two.entity;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public enum StayType {
    ERRONEOUS_DATA,
    SHORT_STAY,
    STANDARD_STAY,
    STANDARD_EXTENDED_STAY,
    LONG_STAY;

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    public String getStay(){
        return this.name();
    }

    public static StayType calculateType(String checkIn, String checkOut){
        if(checkIn == null || checkOut == null){
            return ERRONEOUS_DATA;
        }
        try {
            Date prevDate = format.parse(checkIn);
            Date currDate = format.parse(checkOut);
            long diff = currDate.getTime() - prevDate.getTime();
            long dayDiff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
            if(dayDiff < 2){
                return SHORT_STAY;
            }
            if(dayDiff <= 7){
               return STANDARD_STAY;
            }
            if(dayDiff <= 14){
                return STANDARD_EXTENDED_STAY;
            }
            if(dayDiff <= 30){
                return LONG_STAY;
            }
            return ERRONEOUS_DATA;
        }catch (ParseException e){
            System.out.println("Exception while parsind day");
        }
        return ERRONEOUS_DATA;
    }

}
