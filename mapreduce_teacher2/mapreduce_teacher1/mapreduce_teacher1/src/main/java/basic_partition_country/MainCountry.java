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
 * �û�3,2020/2/11 21:30,�й�
 * �û�3,2020/3/20 11:20,�ݿ�
 * �û�1098,2018/11/29 15:52,Ӣ��
 * ʹ�÷�����������ÿ�����ҵļ�¼�ŵ�һ���ļ��У��ܹ�13������
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

        // ��������/���·��
        setPath(job,configuration);

        // ��jar��
        job.setJarByClass(MainCountry.class);
        // ͨ��job��������/�����ʽ
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // ���ô���Map/Reduce�׶ε���
        job.setMapperClass(LoginMapper.class);
        job.setReducerClass(LoginReducer.class);

        //����map��k2 v2����
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LoginBean.class);

        //����reduce��k2 v2����
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // �����Զ����������
        job.setPartitionerClass(LoginPartition.class);
        // ����ReduceTask������
        job.setNumReduceTasks(13);
        // �ύ��ҵ
        job.waitForCompletion(true);
    }

    public static Configuration getConfig(){
        Configuration  configuration= new Configuration();
        if (isLocal) {
            configuration.set("mapreduce.framework.name", "local"); //����mapreduce���Ϊ����
            configuration.set("fs.defaultF", "file:///"); //�����ļ�ϵͳΪ����windows
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
            //outĿ¼�Ѵ�������ɾ��
            if (fs.exists(outpath)){
                fs.delete(outpath,true);
            }
            FileOutputFormat.setOutputPath(job, new Path(localOutPath));
        }
        else{
            FileInputFormat.setInputPaths(job, new Path(inPath));
            Path outpath = new Path(outPath);
            //outĿ¼�Ѵ�������ɾ��
            if (fs.exists(outpath)){
                fs.delete(outpath,true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
        }
    }
}
