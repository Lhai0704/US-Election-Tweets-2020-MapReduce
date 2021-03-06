import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

// 发布时间统计
// 输出json文件，格式如下：
// { value: ["1997-10-1", 684]},


public class Created_time {


    public static class CreatedTimeMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text created_time = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 正则表达式作用是使转义过的逗号不切分字符串
            String regex = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";
            Pattern p = Pattern.compile(regex);
            String[] data = p.split(value.toString());

            // 切分子字符串，data[0]为2020-10-20 00:32:50
            String time = data[0].substring(0, 10);

            created_time.set(time);
            context.write(created_time, one);
        }
    }

    public static class CreatedTimeReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        Text data = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable val : values) {
                sum += val.get();
            }

            // 输出json文件的一行
            data.set("{ value: [\"" + key.toString() + "\", " + sum + "]},");

            context.write(data, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Created_time");

        job.setJarByClass(Created_time.class);

        job.setMapperClass(CreatedTimeMapper.class);
        job.setReducerClass(CreatedTimeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
