package bd.homework1;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Пользовательский класс Writable для данных, содержащий:
 - bytes - количетсов байт
 - avg_bytes - среднее число байт
 - requests - количество запросов
 */
public class ByteStatWritable implements Writable {
    private IntWritable bytes;
    private FloatWritable avg_bytes;
    private IntWritable requests;

    public ByteStatWritable() {
        bytes = new IntWritable(0);
        avg_bytes = new FloatWritable(0);
        requests = new IntWritable(0);
    }

    public ByteStatWritable(IntWritable bytes, FloatWritable avg_bytes, IntWritable requests) {
        this.bytes = bytes;
        this.avg_bytes = avg_bytes;
        this.requests = requests;
    }

    public IntWritable getBytes() {
        return bytes;
    }

    public FloatWritable getAvgBytes() {
        return avg_bytes;
    }

    public IntWritable getRequests() {
        return requests;
    }

    public void setBytes(IntWritable bytes) {
        this.bytes = bytes;
    }

    public void setAvg_bytes(FloatWritable avg_bytes) {
        this.avg_bytes = avg_bytes;
    }

    public void setRequests(IntWritable requests) {
        this.requests = requests;
    }

    public void readFields(DataInput in) throws IOException {
        bytes.readFields(in);
        avg_bytes.readFields(in);
        requests.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        bytes.write(out);
        avg_bytes.write(out);
        requests.write(out);
    }

    @Override
    public String toString() {
        return bytes.toString() + "," + avg_bytes.toString() + "," + requests.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof ByteStatWritable)
        {
            ByteStatWritable tmp = (ByteStatWritable) o;
            return bytes.equals(tmp.bytes) && avg_bytes.equals(tmp.avg_bytes) && requests.equals(tmp.requests);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytes, avg_bytes, requests);
    }


}
