package basic_group1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/*
 *
 * ��һ���ֶ�Ϊ�����ţ��ڶ����ֶ�Ϊ��Ʒid �������ֶ�Ϊ�۸�������tab���ֿ�
 * Լ��������һ���������԰��������Ʒ��һ����Ʒ��ӦΨһ�ļ۸�
 * Ҫ���ҳ�ÿ����������Ʒ�۸���ߵļ�¼
O00001  123 1234
O00002  124 3435
O00003  125 132.78
O00004  126 334
O00004  127 8976
O00003  128 635
O00002  129 23
O00001  130 980
O00001  131 111
O00002  132 66
O00003  133 42
O00004  134 88
O00005  135 900
������£�
O00001  123 1234.0
O00002  124 3435.0
O00003  128 635.0
O00004  127 8976.0
O00005  135 900.0
*
*/
public class GroupComparatorMain {
    public static boolean isLocal = false;
    public static String localInPath ="D:\\hadoop-local-test\\input";
    public static String localOutPath ="D:\\hadoop-local-test\\output";
    public static String inPath = "hdfs://master:9000/in66/";
    public static String outPath = "hdfs://master:9000/out66/";

    public static void main(String[] args)
            throws IOException, ClassNotFoundException,
            InterruptedException {

        Configuration configuration = getConfig();
        Job job = Job.getInstance(configuration,
                GroupComparatorMain.class.getSimpleName());
        job.setJarByClass(GroupComparatorMain.class);
        //����Mapper���������
        job.setMapperClass(GroupMapper.class);

        // ��������/���·��
        setPath(job,configuration);

        //�������
        job.setGroupingComparatorClass(CustGroupComparator.class);
        //����Reducer���������
        job.setReducerClass(GroupReducer.class);

        //����map��k2 v2����
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //����reduce��k2 v2����
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

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

/**
 * �������
 */
class CustGroupComparator extends WritableComparator{
    public CustGroupComparator() {
        super(OrderBean.class,true);
    }

    //hadoop���ñ��ӿڣ��Ƚ�����bean�Ƿ�Ϊͬһ��key
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean)a;
        OrderBean ob = (OrderBean)b;
        //��hadoop��Ϊorderid��ͬ��beanΪͬһ��key
        return oa.getOrder_id().compareTo(ob.getOrder_id());
    }
}

class OrderBean implements WritableComparable<OrderBean>{
    private String order_id;//������
    private String id ;//��Ʒid
    private double prise;//��Ʒ�ļ۸�

    public OrderBean() {

    }

    public OrderBean(String order_id,String id,double prise) {
        this.order_id = order_id ;
        this.id = id;
        this.prise = prise;
    }

    public String getOrder_id() {
        return order_id;
    }
    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public double getPrise() {
        return prise;
    }
    public void setPrise(double prise) {
        this.prise = prise;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(id);
        out.writeDouble(prise);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readUTF();
        this.id = in.readUTF();
        this.prise = in.readDouble();
    }

    //hadoop���ñ��ӿ�
    @Override
    public int compareTo(OrderBean o) {
        int cnt = this.order_id.compareTo(o.getOrder_id());
        if(cnt==0) {
            if (this.prise>o.getPrise())
            {
                return -1;//��ʾthisС��o����this����o��ǰ�棨��˼۸�Խ�ߵ�����Խǰ�棩
            }
            else{
                return 1;//��ʾthis����o,��this����o�ĺ���
            }
        }
        return cnt;
    }
    @Override
    public String toString() {
        return  order_id + "\t" + id + "\t" + prise ;
    }
}
class GroupMapper extends Mapper<LongWritable, Text,
        OrderBean, NullWritable>{
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text,
                               OrderBean, NullWritable>.Context context)
            throws IOException, InterruptedException {
        String[] split =  value.toString().split(" ");
        OrderBean ob = new OrderBean();
        ob.setOrder_id(split[0]);
        ob.setId(split[1]);
        ob.setPrise(Double.parseDouble(split[2]));
        context.write(ob,  NullWritable.get());
    }
}
class GroupReducer extends Reducer<OrderBean, NullWritable,
        OrderBean, NullWritable>{
    @Override
    protected void reduce(OrderBean bean,
                          Iterable<NullWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        int k = 0;
        //�������values����Ϊ��һ��keyֵ����������
//        for (NullWritable v : values) {
//           System.out.println(bean.toString());
//
//        }
        context.write(bean, NullWritable.get());

    }


}