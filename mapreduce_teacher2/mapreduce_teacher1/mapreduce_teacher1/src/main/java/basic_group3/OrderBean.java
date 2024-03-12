package basic_group3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean>{

    private String orderId;//����id
    private String userId;//�û�id
    private String pdtName;//��Ʒ����
    private float price;//�۸�
    private int number; //����
    private float amountFee;//�����ܼ�

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

    // ֮ǰ�ѵ����˷���������ʹ��id�����˷�������Ȼ���ÿ��������<k,v>���ñ��ӿڽ�������
    // �ȽϹ����ȱȶ����ţ���������ͬ�ģ��۸�ߵ��ź��棬�۸�͵���ǰ��
    @Override
    public int compareTo(OrderBean o) {
        int result = this.orderId.compareTo(o.getOrderId());
        if (result == 0){
            //��������ͬ
            if (this.getAmountFee()>o.getAmountFee())
            {
                return -1;//-1��ʾthis����o��ǰ�棨��˼۸�Խ�ߵ�����Խǰ�棩
            }
            else{
                return 1;//1��ʾthis����o�ĺ���
            }

        }
        else{
            return result;
        }

    }

}
