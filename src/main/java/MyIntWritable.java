import org.apache.hadoop.io.IntWritable;

public class MyIntWritable extends IntWritable {

    public MyIntWritable() {
    }

    public MyIntWritable(int value) {
        super(value);
    }

    @Override
    public int compareTo(IntWritable o) {
        return -super.compareTo(o);  //重写IntWritable排序方法，默认是升序 ，
    }
}