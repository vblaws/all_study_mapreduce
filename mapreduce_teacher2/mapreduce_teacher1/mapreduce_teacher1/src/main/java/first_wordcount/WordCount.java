package first_wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 * 词频统计
 */
public class WordCount {
    public static String inPath = "hdfs://master:8020/input/";
    public static String outPath = "hdfs://master:8020/out22/";
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration,
                WordCount.class.getSimpleName());
        // 设置输入/输出路径
        setPath(job, configuration);

        job.setJarByClass(WordCount.class);
        // 通过job设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置map的k2 v2类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce的k2 v2类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置map类和reduce类
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);

        job.waitForCompletion(true);

    }
    public static Configuration getConfig() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://master:8020/");
        return configuration;
    }
    public static void setPath(Job job, Configuration conf) throws IOException {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileInputFormat.setInputPaths(job, new Path(inPath));
        Path outpath = new Path(outPath);
        //out目录已存在则先删除
        if (fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);
    }
}
class WordMap extends Mapper<Object, Text, Text, IntWritable> {
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split(" ");
        for (String word : lines) {
            // 每个单词出现１次，作为中间结果输出
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : values) {
            sum = sum + count.get();
//            context.write(key, new IntWritable(sum));// 输出最终结果,放到这里会导致重复输出
        }
        context.write(key, new IntWritable(sum));// 输出最终结果
    }
}
