package basic_partition_country;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LoginReducer extends Reducer<Text, LoginBean,
        NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<LoginBean> values,Context context)
            throws IOException, InterruptedException {
        System.out.println("in reduce:--------");
        for (LoginBean bean : values) {
            System.out.println("bean:"+bean.getUserId());
            context.write(NullWritable.get(),
                    new Text(bean.getUserId()+","+
                            bean.getLoginTime()+","+
                            bean.getLoginPlace()));
        }
    }
}
