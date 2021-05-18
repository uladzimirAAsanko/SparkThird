package by.sanko.spark.two.parser;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    private static final char comma = ',';

    public static List<String> parse(String data, int numberOfFields){
        List<String> list = new ArrayList<>();
        int indexOfComma = data.indexOf(comma);
        list.add(data.substring(0, indexOfComma));
        String residue = data.substring(indexOfComma + 1);
        for(int i = 0; i < numberOfFields - 2; i++){
            indexOfComma = residue.indexOf(comma);
            String tmp = residue.substring(0, indexOfComma);
            list.add(residue.substring(0, indexOfComma));
            residue = residue.substring(indexOfComma + 1);
        }
        list.add(residue);
        return list;
    }
}