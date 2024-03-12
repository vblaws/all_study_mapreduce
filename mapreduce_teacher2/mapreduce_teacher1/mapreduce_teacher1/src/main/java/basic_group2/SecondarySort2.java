package basic_group2;

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

/**
 * 二次排序
 * 原始数据：
 1,1
 2,2
 1,3
 2,3
 1,2
 2,1
 * 排序后数据：第一列按从小到大顺序排列，第二列按从大到小排列
 1,3
 1,2
 1,1
 2,3
 2,2
 2,1
 *
 */

/**
 * 分区和分组的区别：
 * 分组发生在reducetask阶段，分组是针对同一个区的数据进行分组。
 * 分组的目的是为了让不同组的数据进入reduce进行处理。
 * 分区发生在maptask阶段，分区的目的是为了让数据进入哪个reducetask。
 */
public class SecondarySort2 {
    public static boolean isLocal = false;
    public static String localInPath ="D:\\hadoop-local-test\\input";
    public static String localOutPath ="D:\\hadoop-local-test\\output";
    public static String inPath = "hdfs://master:9000/in66/";
    public static String outPath = "hdfs://master:9000/out66/";

    public static void main(String[] args) throws Exception {
        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration,
                SecondarySort2.class.getSimpleName());
        job.setJarByClass(SecondarySort2.class);
        //设置Mapper的相关属性
        job.setMapperClass(TheMapper.class);

        // 设置输入/输出路径
        setPath(job,configuration);

        //分组：在reduce阶段执行
        job.setGroupingComparatorClass(KeyComparator.class);
        //设置Reducer的相关属性
        job.setReducerClass(TheReducer.class);

        //设置map的k2 v2类型
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce的k2 v2类型
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

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

    static class TheMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int field1 = Integer.parseInt(fields[0]);
            int field2 = Integer.parseInt(fields[1]);
            context.write(new IntPair(field1,field2), NullWritable.get());
        }
    }
    static class TheReducer extends Reducer<IntPair, NullWritable,IntPair, NullWritable> {
         @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
             System.out.println("in reduce----");
             for(NullWritable v:values){
                 System.out.println("in for: "+ key);
                 context.write(key, NullWritable.get());
             }
        }
    }
}
/**
 * 分组
 */
class KeyComparator extends WritableComparator {
    //无参构造器必须加上，否则报错。
    protected KeyComparator() {
        super(IntPair.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        IntPair ip1 = (IntPair) a;
        IntPair ip2 = (IntPair) b;
        if (ip1.getFirst()==ip2.getFirst()){
            //first相同的为一组
            return 0;
        }else
            //return 1/return -1都不对结果产生影响
            return 1;
    }
}
