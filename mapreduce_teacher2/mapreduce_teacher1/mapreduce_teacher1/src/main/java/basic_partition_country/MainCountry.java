package basic_partition_country;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * user_id,login_time,login_place
 * 用户3,2020/2/11 21:30,中国
 * 用户3,2020/3/20 11:20,捷克
 * 用户1098,2018/11/29 15:52,英国
 * 使用分区操作，将每个国家的记录放到一个文件中，总共13个国家
 */
public class MainCountry
{
    public static boolean isLocal = true;
    public static String localInPath ="D:\\hadoop-local-test\\input";
    public static String localOutPath ="D:\\hadoop-local-test\\output";
    public static String inPath = "hdfs://master:9000/in66/";
    public static String outPath = "hdfs://master:9000/out66/";

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration,
                MainCountry.class.getSimpleName());

        // 设置输入/输出路径
        setPath(job,configuration);

        // 打jar包
        job.setJarByClass(MainCountry.class);
        // 通过job设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置处理Map/Reduce阶段的类
        job.setMapperClass(LoginMapper.class);
        job.setReducerClass(LoginReducer.class);

        //设置map的k2 v2类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LoginBean.class);

        //设置reduce的k2 v2类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置自定义分区的类
        job.setPartitionerClass(LoginPartition.class);
        // 设置ReduceTask的数量
        job.setNumReduceTasks(13);
        // 提交作业
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
