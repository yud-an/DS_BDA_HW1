package bd.homework1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.awt.*;
import java.io.IOException;

/**
 * Маппер: парсит ip-адрес и количество байт по запросу из логов, к каждому ip адресу присваивает
 * количество байт, к количеству запросов единицу, а среднее количество байт рассчитывается в {@link HW1Reducer}
 */
public class HW1Mapper extends Mapper<Object, Text, Text, ByteStatWritable> {

    private Text word = new Text();
    private IntWritable bytes = new IntWritable();
    private FloatWritable avg_bytes = new FloatWritable(0);
    private IntWritable requests = new IntWritable(1);
    private ByteStatWritable stat = new ByteStatWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Регулярное выражение для валидации ipv4
        String ipregex = "\\A(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}\\z";
        String[] arr = line.split(" ");
        // "Валидация" строки из логов
        if(arr.length < 10 || !arr[0].matches(ipregex)) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            word.set(arr[0]);
            bytes.set(Integer.parseInt(arr[9]));

            stat.setBytes(bytes);
            stat.setAvg_bytes(avg_bytes);
            stat.setRequests(requests);

            context.write(word, stat);
            System.out.println(context);
        }
    }
}
