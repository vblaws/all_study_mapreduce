package basic_group3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator {

    public OrderIdGroupingComparator() {
        super(OrderBean.class, true);
    }

    //分组函数调用本接口是为了判断两个bean是否相同，相同则认为这两个bean为同一个分组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //相同的OrderId认为是一个key。
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        System.out.println("in grouping a = " + ((OrderBean) a).toString() +" b= " + ((OrderBean) b).toString());
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}