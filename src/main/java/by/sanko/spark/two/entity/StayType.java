package by.sanko.spark.two.entity;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public enum StayType {
    ERRONEOUS_DATA(0),
    SHORT_STAY(1),
    STANDARD_STAY(2),
    STANDARD_EXTENDED_STAY(3),
    LONG_STAY(4);

    int id;
    StayType(int id){
        this.id = id;
    }

    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");


    public int getStayID(){
        return this.id;
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
