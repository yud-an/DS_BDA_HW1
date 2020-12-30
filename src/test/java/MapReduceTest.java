import bd.homework1.ByteStatWritable;
import bd.homework1.HW1Combiner;
import eu.bitwalker.useragentutils.UserAgent;
import bd.homework1.HW1Mapper;
import bd.homework1.HW1Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author aguminskaya
 * @since 12/10/2018
 */
public class MapReduceTest {

    private MapDriver<Object, Text, Text, ByteStatWritable> mapDriver;
    private ReduceDriver<Text, ByteStatWritable, Text, ByteStatWritable> reduceDriver, combinerDriver;
    private MapReduceDriver<Object, Text, Text, ByteStatWritable, Text, ByteStatWritable> mapReduceDriver;

    private final String testIP = "113.160.201.41 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 3699 \"-\" \"Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/5.0 Opera 11.11\"\n";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        HW1Combiner combiner = new HW1Combiner();
        mapDriver = MapDriver.newMapDriver(mapper);
        combinerDriver = reduceDriver.newReduceDriver(combiner);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("113.160.201.41"), new ByteStatWritable(new IntWritable(3699),new FloatWritable(0),new IntWritable(1)))
                .runTest();
    }

    @Test
    public void testCombiner() throws IOException {
        List<ByteStatWritable> values = new ArrayList<ByteStatWritable>();
        values.add(new ByteStatWritable(new IntWritable(3699),new FloatWritable(0),new IntWritable(1)));
        values.add(new ByteStatWritable(new IntWritable(3699),new FloatWritable(0),new IntWritable(1)));
        combinerDriver
                .withInput(new Text("113.160.201.41"), values)
                .withOutput(new Text("113.160.201.41"), new ByteStatWritable(new IntWritable(3699*2),new FloatWritable(0),new IntWritable(2)))
                .runTest();
    }
    @Test
    public void testReducer() throws IOException {
        List<ByteStatWritable> values = new ArrayList<ByteStatWritable>();
        values.add(new ByteStatWritable(new IntWritable(3699),new FloatWritable(0),new IntWritable(1)));
        values.add(new ByteStatWritable(new IntWritable(3699),new FloatWritable(0),new IntWritable(1)));
        reduceDriver
                .withInput(new Text("113.160.201.41"), values)
                .withOutput(new Text("113.160.201.41"), new ByteStatWritable(new IntWritable(3699*2),new FloatWritable(3699),new IntWritable(2)))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("113.160.201.41"), new ByteStatWritable(new IntWritable(3699*4),new FloatWritable(3699),new IntWritable(4)))
                .runTest();
    }
}
