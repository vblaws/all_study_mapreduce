package basic_max;

import basic_average.Average;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 *计算1.txt和2.txt的最大值
 */
public class Max {
    public static boolean isLocal = false;
    public static String localInPath ="D:\\hadoop-local-test\\input";
    public static String localOutPath ="D:\\hadoop-local-test\\output";
    public static String inPath = "hdfs://master:9000/in66/";
    public static String outPath = "hdfs://master:9000/out66/";

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration, Max.class.getSimpleName());
        job.setJarByClass(Max.class);
        // 设置输入/输出路径
        setPath(job,configuration);

        //设置自定义Mapper类和设置map函数输出数据的key和value的类型
        job.setMapperClass(MaxMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定Reducer类和输出key和value的类型
        job.setReducerClass(MaxReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }

    public static Configuration getConfig(){
        Configuration  configuration= new Configuration();
        if (isLocal) {
            configuration.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
            configuration.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        }
        else{
            configuration.set("fs.defaultFS","hdfs://master:9000/");
        }
        return configuration;
    }

    public static void setPath(Job job,Configuration  conf) throws IOException {
        FileSystem fs =null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (isLocal)
        {
            FileInputFormat.setInputPaths(job, new Path(localInPath));
            Path outpath = new Path(localOutPath);
            //out目录已存在则先删除
            if (fs.exists(outpath)){
                fs.delete(outpath,true);
            }
            FileOutputFormat.setOutputPath(job, new Path(localOutPath));
        }
        else{
            FileInputFormat.setInputPaths(job, new Path(inPath));
            Path outpath = new Path(outPath);
            //out目录已存在则先删除
            if (fs.exists(outpath)){
                fs.delete(outpath,true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
        }
    }
}
//                             k1,            v1,      k2
class MaxMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

        @Override
        //                            k1      v1
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException,
                InterruptedException {
            // 获取输入的行
            String line = value.toString();
            // 抛弃无效记录
            if (line == null || line.equals("")) {
                return;
            }
            // 把line转换为数值
            long temp = Long.parseLong(line);
            context.write(new LongWritable(temp), NullWritable.get());
        }
}

/**
 * 汇总多个任务产生的最大值，再次比较
 */
class MaxReducer extends Reducer<LongWritable, NullWritable,
        LongWritable, NullWritable> {
    // 定义一个临时变量
    private Long max = Long.MIN_VALUE;
    protected void reduce(LongWritable key, Iterable<NullWritable> value,
                          Context context)
            throws IOException, InterruptedException {
          max = key.get();
    }
    /**
     * reduce任务完成后调用本接口：将reduce的最大值写到hdfs
     */
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // 设置最大值
        LongWritable maxValue = new LongWritable();
        maxValue.set(max);
        context.write(maxValue, NullWritable.get());
    }
}

