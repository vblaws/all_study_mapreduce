package basic_group2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;
    //注意：需要添加无参的构造方法，否则反射时会报错。
    public IntPair() {

    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }



    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }
    public void readFields(DataInput in) throws IOException {
        this.first = in.readInt();
        this.second = in.readInt();
    }

    public String toString() {
        return first + "," + second;
    }

    @Override
    public int compareTo(IntPair tp) {
        //第一列按升序排列
        if (this.first < tp.first){
            return -1; //first小的this，排前面
        }
        else if (this.first > tp.first){
            return 1;//first大的this，排后面
        }
        else{
            //first相同的，则比较second
            if (this.second > tp.second) {
                return -1; //second大的排前面
            } else {
                return 1; //second小的排后面
            }
        }
    }
}
