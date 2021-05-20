import by.sanko.spark.two.entity.HotelData;
import by.sanko.spark.two.parser.HotelParser;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class TestParser {
    @Test
    public void testParserEmpty(){
        HotelData expected = null;
        HotelData actual = HotelParser.parseData("");
        assertEquals(actual, expected);
    }

    @Test
    public void testParserNull(){
        HotelData expected = null;
        HotelData actual = HotelParser.parseData(null);
        assertEquals(actual, expected);
    }

    @Test
    public void testParserCorrect(){
        HotelData expected = new HotelData(12, "hotel", "blr", "minsk", "address house", 25, 12, "ggwc");
        HotelData actual = HotelParser.parseData("12,hotel,blr,minsk,address house,25,12,ggwc");
        assertEquals(actual, expected);
    }
}
