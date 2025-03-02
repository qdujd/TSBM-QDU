package cn.edu.ruc.utils;

/*
 * 工具类，用于从接口中获得两个返回值
*/
public class ResultUtils {
    private long value1;
    private long value2;
 
    public ResultUtils(long value, long timeout) {
        this.value1 = value;
        this.value2 = timeout;
    }
 
    public long getValue() {
        return value1;
    }

    public long getTimeout() {
        return value2;
    }
}
