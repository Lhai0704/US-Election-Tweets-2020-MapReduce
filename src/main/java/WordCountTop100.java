import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 推文词频Top100

public class WordCountTop100 {

    private static final int K = 100;

    public static class MyMapper extends Mapper<LongWritable, Text, MyIntWritable, Text> {

        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");

            text.set(data[0]);

            context.write(new MyIntWritable(Integer.parseInt(data[1])), text);
        }
    }

    public static class MyReducer extends Reducer<MyIntWritable, Text, Text, NullWritable> {

        int num = 0;
        String str;

        @Override
        protected void reduce(MyIntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text text : values) {
                if (num < K) {
                    str = text.toString();
                    // { "name": "aa", "value": 123},
                    context.write(new Text("{ \"name\": \"" + str.substring(1, str.length() - 1) + "\", \"value\": " + key.get() + "},"), NullWritable.get());
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

        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileSystem fileSystem = FileSystem.get(conf);

        fileSystem.deleteOnExit(new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // exit(arg) arg 非0表示jvm异常终止
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}

