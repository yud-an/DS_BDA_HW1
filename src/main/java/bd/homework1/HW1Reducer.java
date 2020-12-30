package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: суммирует все байты и запросы полученные от {@link HW1Mapper}, после делит первое на второе и
 * записывает среднее количество байт по всем запросам для каждого ip-адреса и выдает результаты
 */
public class HW1Reducer extends Reducer<Text, ByteStatWritable, Text, ByteStatWritable> {

    private ByteStatWritable stat = new ByteStatWritable();

    @Override
    protected void reduce(Text key, Iterable<ByteStatWritable> values, Context context) throws IOException, InterruptedException {
        int totalBytes = 0;
        int requests = 0;

        for(ByteStatWritable iter : values) {
            totalBytes += iter.getBytes().get();
            requests += iter.getRequests().get();
        }
        stat.setBytes(new IntWritable(totalBytes));
        stat.setAvg_bytes(new FloatWritable(totalBytes/requests));
        stat.setRequests((new IntWritable(requests)));


        context.write(key, stat);
    }
}
