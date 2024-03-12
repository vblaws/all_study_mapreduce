package basic_clean1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map.Entry;

/**已知有如下数据，每条包括应该包括3个字段，如果某条记录有字段缺失的，则视该记录为无效记录，
 * 对这些数据进行清洗，将无效记录删除，只保留有效的数据，并且在程序运行控制台输出无效记录的数量
 * 张三,20,计算机应用专业
 * 李四,21,大数据技术专业
 * 陈先,20,软件技术专业
 * 李刚,,计算机网络技术专业
 * ,30
 * 马斌,21,移动开发专业
 * 清洗后的结果：
 * 张三,20,计算机应用专业
 * 李四,21,大数据技术专业
 * 陈先,20,软件技术专业
 * 马斌,21,移动开发专业
 */
public class Clean1{

    public static boolean isLocal = true;
    public static String localInPath ="D:\\hadoop-local-test\\input";
    public static String localOutPath ="D:\\hadoop-local-test\\output";
    public static String inPath = "hdfs://master:9000/dongzhiyong/";
    public static String outPath = "hdfs://master:9000/dongzhiyog66/";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration,basic_clean1.Clean1.class.getSimpleName());
        // 设置输入/输出路径
        setPath(job,configuration);

        job.setJarByClass(basic_clean1.Clean1.class);
        // 通过job设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置map的k2 v2类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce的k2 v2类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置map类和reduce类
        job.setMapperClass(basic_clean1.CleanDataMapper.class);
        job.setReducerClass(basic_clean1.CleanDataReducer.class);

        job.waitForCompletion(true);
        //hadoop程序执行完成后，通过计时器获得无效记录的数量
        System.out.println("invalid records:" + job.getCounters().findCounter(CleanDataMapper.CleanRecorder.InvalidCounter).getValue());
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

class CleanDataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    //使用枚举记录无效记录数量
    public static enum CleanRecorder{
        InvalidCounter
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String record = value.toString();
        String[] fields = record.split(",");

        //过滤掉无效数据
        if(fields.length != 3) {
            //使用计时器统计无效记录数量
            context.getCounter(CleanRecorder.InvalidCounter).increment(1);
            return;
        }
        String name = fields[0];
        String age = fields[1];
        String professional = fields[2];
        if (name.length()==0||age.length()==0 ||professional.length()==0){
            //使用计时器统计无效记录数量
            context.getCounter(CleanRecorder.InvalidCounter).increment(1);
            return;
        }
        //输出清洗后的数据
        context.write(new Text(name+","+ age+","+professional),NullWritable.get());
    }
}

class CleanDataReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //将清洗后的数据保存到hdfs中
        context.write(key, NullWritable.get());
    }
}
