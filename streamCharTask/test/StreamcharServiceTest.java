import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamcharServiceTest {

@Test
public void testStreamcharService(){
    StreamcharService streamcharService = new StreamcharService(new StreamcharImpl());
    Assertions.assertEquals(streamcharService.solve("aab"),"a#b");
}

    @Test
    public void testStreamcharServiceEmptyChars(){
        StreamcharService streamcharService = new StreamcharService(new StreamcharImpl());
        Assertions.assertEquals(streamcharService.solve("   "),"###");
    }

    @Test
    public void testStreamcharServiceBoundaryCases() {
        Assertions.assertEquals(new StreamcharService(new StreamcharImpl()).solve("bbbbk"),"b###k");
        Assertions.assertEquals(new StreamcharService(new StreamcharImpl()).solve("abcdef"),"aaaaaa");
    }

}
