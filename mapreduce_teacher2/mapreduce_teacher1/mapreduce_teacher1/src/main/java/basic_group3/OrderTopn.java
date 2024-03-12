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

/**订单id,用户id，商品，单价，数量
 * order001,u001,小米6,1999.9,2
 * order001,u001,雀巢咖啡,99.0,2
 * order001,u001,安慕希,250.0,2
 * order001,u001,经典红双喜,200.0,4
 * order001,u001,防水电脑包,400.0,2
 * order002,u002,小米手环,199.0,3
 * order002,u002,榴莲,15.0,10
 * order002,u002,苹果,4.5,20
 * order002,u002,肥皂,10.0,40
 *
 * 需要求出每一个订单中成交金额最大的三笔,比如：
 * 订单id，用户id，商品，单价，数量，总价（=单价*数量）
 * order002,u002,小米手环,199.0,3,597.0
 * order002,u002,肥皂,10.0,40,400.0
 * order002,u002,榴莲,15.0,10,150.0
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
        //设置Mapper的相关属性
        job.setMapperClass(OrderTopnMapper.class);

        // 设置输入/输出路径
        setPath(job,configuration);

        //分组操作
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
        //设置Reducer的相关属性
        job.setReducerClass(OrderTopnReducer.class);

        //设置map的k2 v2类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce的k2 v2类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.waitForCompletion(true);
    }

    public static Configuration getConfig() {
        Configuration configuration = new Configuration();
        if (isLocal) {
            configuration.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
            configuration.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
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
            //out目录已存在则先删除
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, new Path(localOutPath));
        } else {
            FileInputFormat.setInputPaths(job, new Path(inPath));
            Path outpath = new Path(outPath);
            //out目录已存在则先删除
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
         * 虽然reduce方法中的参数key只有一个，但是只要迭代器迭代一次，key中的值就会变
         */
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values,
                              Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
            int i=0;
            for (NullWritable v : values) {
                System.out.println("in reduce key = " + key.toString());
                context.write(key, v);
                //只输出前3个值
                if(++i==3) {
                    return;
                }
            }
        }
}
