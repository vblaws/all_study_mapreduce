package basic_group3;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**����id,�û�id����Ʒ�����ۣ�����
 * order001,u001,С��6,1999.9,2
 * order001,u001,ȸ������,99.0,2
 * order001,u001,��Ľϣ,250.0,2
 * order001,u001,�����˫ϲ,200.0,4
 * order001,u001,��ˮ���԰�,400.0,2
 * order002,u002,С���ֻ�,199.0,3
 * order002,u002,����,15.0,10
 * order002,u002,ƻ��,4.5,20
 * order002,u002,����,10.0,40
 *
 * ��Ҫ���ÿһ�������гɽ������������,���磺
 * ����id���û�id����Ʒ�����ۣ��������ܼۣ�=����*������
 * order002,u002,С���ֻ�,199.0,3,597.0
 * order002,u002,����,10.0,40,400.0
 * order002,u002,����,15.0,10,150.0
 */
public class OrderTopn {
    public static boolean isLocal = true;
    public static String localInPath ="D:\\hadoop-local-test\\input";
    public static String localOutPath ="D:\\hadoop-local-test\\output";
    public static String inPath = "hdfs://master:9000/in66/";
    public static String outPath = "hdfs://master:9000/out66/";

    public static void main(String[] args) throws Exception {
        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration, OrderTopn.class.getSimpleName());
        job.setJarByClass(OrderTopn.class);
        //����Mapper���������
        job.setMapperClass(OrderTopnMapper.class);

        // ��������/���·��
        setPath(job,configuration);

        //�������
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
        //����Reducer���������
        job.setReducerClass(OrderTopnReducer.class);

        //����map��k2 v2����
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //����reduce��k2 v2����
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.waitForCompletion(true);
    }

    public static Configuration getConfig() {
        Configuration configuration = new Configuration();
        if (isLocal) {
            configuration.set("mapreduce.framework.name", "local"); //����mapreduce���Ϊ����
            configuration.set("fs.defaultF", "file:///"); //�����ļ�ϵͳΪ����windows
        } else {
            configuration.set("fs.defaultFS", "hdfs://master:9000/");
        }
        return configuration;
    }

    public static void setPath(Job job, Configuration conf) throws IOException {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (isLocal) {
            FileInputFormat.setInputPaths(job, new Path(localInPath));
            Path outpath = new Path(localOutPath);
            //outĿ¼�Ѵ�������ɾ��
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, new Path(localOutPath));
        } else {
            FileInputFormat.setInputPaths(job, new Path(inPath));
            Path outpath = new Path(outPath);
            //outĿ¼�Ѵ�������ɾ��
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
        }
    }
}
class OrderTopnMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
        OrderBean orderBean = new OrderBean();
        NullWritable v = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            orderBean.set(fields[0], fields[1], fields[2],
                    Float.parseFloat(fields[3]),
                    Integer.parseInt(fields[4]));

            context.write(orderBean,v);
        }
}
class OrderTopnReducer extends Reducer< OrderBean, NullWritable,  OrderBean, NullWritable>{

        /**
         * ��Ȼreduce�����еĲ���keyֻ��һ��������ֻҪ����������һ�Σ�key�е�ֵ�ͻ��
         */
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values,
                              Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
            int i=0;
            for (NullWritable v : values) {
                System.out.println("in reduce key = " + key.toString());
                context.write(key, v);
                //ֻ���ǰ3��ֵ
                if(++i==3) {
                    return;
                }
            }
        }
}
