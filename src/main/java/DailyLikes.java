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

// 每日点赞数
// 输出json文件：{ "value": ["1997-10-1", 684]},


public class DailyLikes {


    public static class DailyLikesMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable likes = new IntWritable();
        private Text created_time = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String regex = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";
            Pattern p = Pattern.compile(regex);
            String[] data = p.split(value.toString());

            String time = data[0].substring(0, 10);
            likes.set((int) Double.parseDouble(data[2]));
            created_time.set(time);
            context.write(created_time, likes);
        }
    }

    public static class DailyLikesReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        Text data = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable val : values) {
                sum += val.get();
            }

//            { "value": ["1997-10-1", 684]},

            data.set("{ \"value\": [\"" + key.toString() + "\", " + sum + "]},");

            context.write(data, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "DailyLikes");


        job.setJarByClass(DailyLikes.class);

        job.setMapperClass(DailyLikesMapper.class);
        job.setReducerClass(DailyLikesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
