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

// 每个州的转发数

public class RetweetCountByStates {

    public static class HeatMapMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable retweet = new IntWritable();
        private Text state = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String regex = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";
            Pattern p = Pattern.compile(regex);
            String[] data = p.split(value.toString());

            retweet.set((int) Double.parseDouble(data[3]));
            if("".equals(data[15])) {

            }else {
                state.set(data[15]);
                context.write(state, retweet);
            }
        }
    }

    public static class HeatMapReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        Text data = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            data.set(key);

            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }

//            { "value": ["Florida", 168746]},
            data.set("{ \"value\": [\"" + data.toString() + "\", " + sum + "]},");


            context.write(data, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RetweetCountByStates");

        job.setJarByClass(RetweetCountByStates.class);

        job.setMapperClass(HeatMapMapper.class);
//        job.setCombinerClass(HeatMapReducer.class);
        job.setReducerClass(HeatMapReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
