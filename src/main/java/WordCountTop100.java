import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class WordCountTop100 {

    public static final int K = 100;

    public static class MyIntWritable extends IntWritable {

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


    public static class MyMapper extends Mapper<LongWritable, Text, MyIntWritable, Text> {

        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");
//            System.out.println(data[0]);

            text.set(data[0]);

            context.write(new MyIntWritable(Integer.parseInt(data[1])), text);

        }


    }

    public static class MyReducer extends Reducer<MyIntWritable, Text, Text, MyIntWritable> {

        int num = 0;

        @Override
        protected void reduce(MyIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text text : values) {
                if (num < K) {
                    context.write(text, key);
                }
                num++;
            }
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);

        job.setJarByClass(LikesTop10.class);


        job.setMapperClass(MyMapper.class);

        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(MyIntWritable.class);

        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(MyIntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileSystem fileSystem = FileSystem.get(conf);

        fileSystem.deleteOnExit(new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // exit(arg) arg 非0表示jvm异常终止
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}

