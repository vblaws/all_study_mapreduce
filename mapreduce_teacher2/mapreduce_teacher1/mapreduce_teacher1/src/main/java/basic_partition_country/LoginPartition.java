package basic_partition_country;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * �Զ������
 * ��ͬ����һ������
 */
public class LoginPartition extends Partitioner<Text, LoginBean> {

    private static HashMap<String,Integer> map = new HashMap<String, Integer>();
    static {
        map.put("�й�",0);
        map.put("�ݿ�",1);
        map.put("Ӣ��",2);
        map.put("Ų��",3);
        map.put("�¹�",4);
        map.put("����",5);
        map.put("����",6);
        map.put("̩��",7);
        map.put("�Ϸ�",8);
        map.put("���",9);
        map.put("��ʿ",10);
        map.put("ϣ��",11);
        map.put("Խ��",12);
    }

    @Override
    public int getPartition(Text text, LoginBean loginBean, int i) {
        String key = text.toString();
        return map.get(key);

    }
}
