package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
//UDF：user defined function
public class PhoneNumParse extends UDF {

    static HashMap<String, String> phoneMap = new HashMap<String, String>();

    static{
        phoneMap.put("136", "beijing");
        phoneMap.put("137", "shanghai");
        phoneMap.put("138", "shenzhen");
    }

    public static String evaluate(int phoneNum) {

        String num = String.valueOf(phoneNum);
        String province = phoneMap.get(num.substring(0, 3));
        return province==null?"foreign":province;
    }
}





