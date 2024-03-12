package basic_group3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator {

    public OrderIdGroupingComparator() {
        super(OrderBean.class, true);
    }

    //���麯�����ñ��ӿ���Ϊ���ж�����bean�Ƿ���ͬ����ͬ����Ϊ������beanΪͬһ������
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //��ͬ��OrderId��Ϊ��һ��key��
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        System.out.println("in grouping a = " + ((OrderBean) a).toString() +" b= " + ((OrderBean) b).toString());
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}