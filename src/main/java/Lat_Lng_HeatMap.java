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

// 提取经纬度，输出为json文件
// {"lat":-0.0101331,"lng":51.4624325,"count":34},

public class Lat_Lng_HeatMap {

    public static class HeatMapMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text lat_lng = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String regex = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";
            Pattern p = Pattern.compile(regex);
            String[] data = p.split(value.toString());

            if(!"".equals(data[11])) {
                lat_lng.set(data[11] + "," + data[10]);
                context.write(lat_lng, one);
            }
        }
    }

    public static class HeatMapReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        Text data = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] latlng = key.toString().split(",");
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }

            data.set("{\"lat\":" + latlng[1] + ",\"lng\":" + latlng[0] + ",\"count\":" + sum + "},");

            context.write(data, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HeatMap");

        job.setJarByClass(Lat_Lng_HeatMap.class);

        job.setMapperClass(HeatMapMapper.class);
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
