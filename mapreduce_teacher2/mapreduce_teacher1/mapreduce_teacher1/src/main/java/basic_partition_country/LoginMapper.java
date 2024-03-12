package basic_partition_country;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LoginMapper extends Mapper<LongWritable,
        Text, Text,LoginBean> {

    @Override
    protected void map(LongWritable key,
                       Text value, Context context)
            throws IOException, InterruptedException {
        //跳过表头
        if(key.get()==0){
            return;
        }
        //执行正常业务
        String[] data = value.toString().split(",");
        LoginBean loginBean = new LoginBean();
        loginBean.setUserId(data[0]);
        loginBean.setLoginTime(data[1]);
        loginBean.setLoginPlace(data[2]);
        String place = loginBean.getLoginPlace();
        context.write(new Text(place),loginBean);
    }
}