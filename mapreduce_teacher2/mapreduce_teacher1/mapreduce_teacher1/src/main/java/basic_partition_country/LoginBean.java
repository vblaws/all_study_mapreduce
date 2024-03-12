package basic_partition_country;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LoginBean implements WritableComparable<LoginBean> {

    //user_id
    private String userId;
    //login_time
    private String loginTime;
    //login_place
    private String loginPlace;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(String loginTime) {
        this.loginTime = loginTime;
    }

    public String getLoginPlace() {
        return loginPlace;
    }

    public void setLoginPlace(String loginPlace) {
        this.loginPlace = loginPlace;
    }

    @Override
    public int compareTo(LoginBean o) {
        return this.loginPlace.compareTo(o.loginPlace);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(loginTime);
        dataOutput.writeUTF(loginPlace);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readUTF();
        loginTime = dataInput.readUTF();
        loginPlace = dataInput.readUTF();
    }
}
