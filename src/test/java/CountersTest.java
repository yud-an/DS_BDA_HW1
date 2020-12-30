import bd.homework1.ByteStatWritable;
import bd.homework1.CounterType;
import eu.bitwalker.useragentutils.UserAgent;
import bd.homework1.HW1Mapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author aguminskaya
 * @since 12/10/2018
 */
public class CountersTest {

    private MapDriver<Object, Text, Text, ByteStatWritable> mapDriver;

    private final String testMalformedIP1 = "mama mila ramu";
    private final String testMalformedIP2 = "113.160.201.41 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200";
    private final String testMalformedIP3 = "113.160.20141 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200";
    private final String testIP = "113.160.201.41 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 3699 \"-\" \"Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/5.0 Opera 11.11\"\n";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterThree() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedIP1))
                .withInput(new LongWritable(), new Text(testMalformedIP2))
                .withInput(new LongWritable(), new Text(testMalformedIP3))
                .runTest();
        assertEquals("Expected 3 counter increment", 3, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("113.160.201.41"), new ByteStatWritable(new IntWritable(3699),new FloatWritable(0),new IntWritable(1)))
                .runTest();
        assertEquals("Expected 0 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testMalformedIP1))
                .withInput(new LongWritable(), new Text(testMalformedIP2))
                .withInput(new LongWritable(), new Text(testMalformedIP3))
                .withOutput(new Text("113.160.201.41"),new ByteStatWritable(new IntWritable(3699),new FloatWritable(0),new IntWritable(1)))
                .runTest();
        assertEquals("Expected 3 counter increment", 3, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}

