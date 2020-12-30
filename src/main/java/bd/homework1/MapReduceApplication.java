package bd.homework1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "total and average bytes per IP");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setCombinerClass(HW1Combiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ByteStatWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        long start = System.currentTimeMillis();
        job.waitForCompletion(true);
        long time = System.currentTimeMillis() - start;
        log.info("=====================JOB ENDED=====================");
        Counter counter = job.getCounters().findCounter((CounterType.MALFORMED));
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "======================");
        log.info("Time: " +String.valueOf(time/1000) + "sec."); // Execution time
    }
}
