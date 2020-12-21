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

// 推文词频

public class WordCount {


    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text text = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 正则表达式作用是使转义过的逗号不切分字符串
            String regex = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";
            Pattern p = Pattern.compile(regex);
            String[] data = p.split(value.toString());
            if (data[17].length() > 4) {
                String[] words = data[17].substring(2, data[17].length() - 2).split(", ");

                for (String word:
                        words) {
                    text.set(word);
                    context.write(text, one);
                }
            }

        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for(IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

//            System.out.println(key.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(Created_time.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
