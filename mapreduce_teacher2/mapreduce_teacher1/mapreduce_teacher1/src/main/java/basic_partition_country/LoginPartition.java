package basic_partition_country;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 自定义分区
 * 相同城市一个分区
 */
public class LoginPartition extends Partitioner<Text, LoginBean> {

    private static HashMap<String,Integer> map = new HashMap<String, Integer>();
    static {
        map.put("中国",0);
        map.put("捷克",1);
        map.put("英国",2);
        map.put("挪威",3);
        map.put("德国",4);
        map.put("波兰",5);
        map.put("荷兰",6);
        map.put("泰国",7);
        map.put("南非",8);
        map.put("瑞典",9);
        map.put("瑞士",10);
        map.put("希腊",11);
        map.put("越南",12);
    }

    @Override
    public int getPartition(Text text, LoginBean loginBean, int i) {
        String key = text.toString();
        return map.get(key);

    }
}
