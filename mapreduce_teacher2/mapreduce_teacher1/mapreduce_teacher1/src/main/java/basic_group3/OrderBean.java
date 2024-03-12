package basic_group3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean>{

    private String orderId;//订单id
    private String userId;//用户id
    private String pdtName;//商品名称
    private float price;//价格
    private int number; //数量
    private float amountFee;//订单总价

    public void set(String orderId, String userId, String pdtName, float price, int number) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.number = number;
        this.amountFee = price * number;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPdtName() {
        return pdtName;
    }

    public void setPdtName(String pdtName) {
        this.pdtName = pdtName;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public float getAmountFee() {
        return amountFee;
    }

    public void setAmountFee(float amountFee) {
        this.amountFee = amountFee;
    }

    @Override
    public String toString() {

        return this.orderId + "," + this.userId + "," + this.pdtName + "," + this.price + "," + this.number + ","
                + this.amountFee;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.price);
        out.writeUTF(this.orderId);
        out.writeUTF(this.userId);
        out.writeUTF(this.pdtName);
        out.writeInt(this.number);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.price = in.readFloat();
        this.orderId = in.readUTF();
        this.userId = in.readUTF();
        this.pdtName = in.readUTF();
        this.number = in.readInt();
        this.amountFee = this.price * this.number;
    }

    // 之前已调用了分区函数（使用id进行了分区），然后对每个分区的<k,v>调用本接口进行排序
    // 比较规则：先比订单号，订单号相同的，价格高的排后面，价格低的排前面
    @Override
    public int compareTo(OrderBean o) {
        int result = this.orderId.compareTo(o.getOrderId());
        if (result == 0){
            //订单号相同
            if (this.getAmountFee()>o.getAmountFee())
            {
                return -1;//-1表示this排在o的前面（因此价格越高的排在越前面）
            }
            else{
                return 1;//1表示this排在o的后面
            }

        }
        else{
            return result;
        }

    }

}
