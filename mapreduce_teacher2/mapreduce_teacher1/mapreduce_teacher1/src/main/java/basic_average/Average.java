package basic_average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 计算每个人的平均成绩
 */
public class Average
{
    public static boolean isLocal = true;
    public static String localInPath ="D:\\hadoop-local-test\\input";
    public static String localOutPath ="D:\\hadoop-local-test\\output";
    public static String inPath = "hdfs://master:9000/in66/";
    public static String outPath = "hdfs://master:9000/out66/";

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException
    {
        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration,
                Average.class.getSimpleName());
        job.setJarByClass(Average.class);

        job.setMapperClass(AvgMap.class);
        job.setReducerClass(AvgReduce.class);

        //设置map的k2 v2类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce的k2 v2类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // 通过job设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入/输出路径
        setPath(job,configuration);

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

class AvgMap extends Mapper<Object, Text, Text, IntWritable>
{
    //                        k1        v1(每一行)
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String [] line = value.toString().split(" ");
        String name = line[0];//姓名
        int score = Integer.parseInt(line[1]);//成绩
        //                   k2              v2
        context.write(new Text(name), new IntWritable(score));
    };
}
//                             k1        v1            k2     v2
class AvgReduce extends Reducer<Text,   IntWritable,  Text, FloatWritable> {
    //                       k1,           v1
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException,
            InterruptedException {
        int sum = 0;
        int count = 0;
        float avg = 0;

        Iterator<IntWritable> iterator = values.iterator();
        //成绩累加
        while (iterator.hasNext()) {
            sum += iterator.next().get();
            count++;
        }
        //计算平均值
        avg =  sum / count;
        //保存到hdfs上   k2,v2
        context.write(key, new FloatWritable(avg));
    }
}