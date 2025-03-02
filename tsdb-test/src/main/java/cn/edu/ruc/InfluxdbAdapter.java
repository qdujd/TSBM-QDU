package cn.edu.ruc;

import cn.edu.ruc.utils.ResultUtils;
import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import okhttp3.*;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.List;
import java.util.concurrent.TimeUnit;


// get set control + return
//自动删除无效引用  control+option+o
//格式化 Command + Option + L
public class InfluxdbAdapter implements BaseAdapter {// ctrl+i 快速实现接口
    private String writeURL = "";
    private String queryURL = "";
    private String dbName = "ruc_test";
    MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain");
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private org.influxdb.InfluxDB INFLUXDB = null;
    private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
            .readTimeout(50000, TimeUnit.MILLISECONDS)
            .connectTimeout(50000, TimeUnit.MILLISECONDS)
            .writeTimeout(50000, TimeUnit.MILLISECONDS)
            .build();

    public static OkHttpClient getOkHttpClient() {
        return OK_HTTP_CLIENT;
    }

    private long exeOkHttpRequest(Request request) {
        long costTime = 0L;
        Response response;
        OkHttpClient client = getOkHttpClient();
        try {
            long startTime1 = System.nanoTime();
            response = client.newCall(request).execute();
            int code = response.code();
            response.close();
            long endTime1 = System.nanoTime();
            costTime = endTime1 - startTime1;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
        return costTime / 1000 / 1000;
    }

    public long execQuery(String query) {
        long costTime = 0L;
        try {
            long startTime1 = System.nanoTime();
            QueryResult results = INFLUXDB.query(new Query(query, dbName));
            long endTime1 = System.nanoTime();
            costTime = endTime1 - startTime1;
            if (results.hasError()) {
                return -1;
            } else {
                return costTime / 1000 / 1000;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }
    public ResultUtils execQuery1(String query) {
        long costTime = 0L;
        long result = 0L;
        try {
            long startTime1 = System.nanoTime();
            QueryResult results = INFLUXDB.query(new Query(query, dbName));
            List<List<Object>> rows = results.getResults().get(0).getSeries().get(0).getValues();
            for (List<Object> row : rows) {
                // 假设每一行数据都包含时间戳和你感兴趣的字段值
                // 注意：时间戳可能不在结果中，取决于你的查询是否请求了它
                Object value = row.get(1); // 字段值通常位于第二列（索引1），如果包含时间戳则位于第三列之后
                // 尝试将字段值转换为long类型
                if (value instanceof Number) {
                    result = ((Number) value).longValue();
                    // 输出或处理long类型的字段值
                    System.out.println("Field Value (long): " + value);
                } else {
                    // 如果字段值不是Number类型，则进行错误处理或输出警告
                    System.err.println("Field value is not a number: " + value.getClass().getName());
                }
            }
            long endTime1 = System.nanoTime();
            costTime = endTime1 - startTime1;
            if (results.hasError()) {
                return new ResultUtils(-1, -1);
            } else {
                costTime = costTime / 1000 / 1000;
                return new ResultUtils(result, costTime);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new ResultUtils(-1, -1);
        }
    }
    public long query1(long start, long end) {
        String sql = "select * from sensor where f='%s' and d='%s' and s='%s' and time>=%s and time<=%s";
        String eSql = String.format(sql, "f1", "d2", "s1", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }

    public long query2(long start, long end, double value) {
        String sqlFormat = "select * from sensor where f='%s' and s='%s' and value>=%s and time>=%s and time<%s";
        String eSql = String.format(sqlFormat, "f1", "d2", value,
                TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }

    public long query3(long start, long end) {
        String sqlFormat = "select mean(value) from sensor where f='%s' and s='%s' and time>=%s and time<=%s group by " +
                "f,d,s,time(1h)";
        String eSql = String.format(sqlFormat, "f1", "s1", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        System.out.println(eSql);
        return execQuery(eSql);
    }

    public long query4(long start, long end) {
        String sqlFormat = "select * from sensor where f='%s' and (s='%s' or s='%s' or s='%s' or s='%s' or s='%s') " +
                "and time>=%s and time<=%s";
        String eSql = String.format(sqlFormat, "f1", "s1", "s2", "s3", "s4", "s5", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }

    public long query5(long start, long end) {
        String sqlFormat = "select * from sensor where f='%s' and time>=%s and time<=%s";
        String eSql = String.format(sqlFormat, "f1", TimeUnit.MILLISECONDS.toNanos(start),
                TimeUnit.MILLISECONDS.toNanos(end));
        return execQuery(eSql);
    }
    @Override
    public ResultUtils query6() {
        String eSql = "select count(*) from sensor";
        System.out.println(eSql);
        return execQuery1(eSql);
    }

    public void initConnect(String ip, String port, String user, String password) {
        String baseUrl = String.format("http://%s:%s", ip, port);
        this.writeURL = baseUrl + "/write?precision=ms&db=" + dbName;
        this.queryURL = baseUrl + "/query?db=" + dbName;
        INFLUXDB = InfluxDBFactory.connect(baseUrl, new OkHttpClient().newBuilder()
                .readTimeout(50000, TimeUnit.MILLISECONDS)
                .connectTimeout(50000, TimeUnit.MILLISECONDS)
                .writeTimeout(50000, TimeUnit.MILLISECONDS));
        INFLUXDB.setDatabase(dbName);
        INFLUXDB.createDatabase(dbName);
    }

    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR);
        StringBuilder sc = new StringBuilder();
        for (String row : rows) {
            String[] sensors = row.split(TSBM.SEPARATOR);
            if (sensors.length < 3) {//过滤空行
                continue;
            }
            String timestamp = sensors[0];
            String farmId = sensors[1];
            String deviceId = sensors[2];
            int length = sensors.length;
            for (int index = 3; index < length; index++) {
                String value = sensors[index];
                String sensorName = "s" + (index - 2);
                sc.append("sensor");
                sc.append(",");
                sc.append("f=");
                sc.append(farmId);
                sc.append(",");
                sc.append("d=");
                sc.append(deviceId);
                sc.append(",");
                sc.append("s=");
                sc.append(sensorName);
                sc.append(" ");

//                sc.append(sensorName);
//                sc.append("=");
                sc.append("value=");
                sc.append(value);
                sc.append(" ");
                sc.append(timestamp);
                sc.append("\n");
            }
        }
        Request request = new Request.Builder()
                .url(writeURL)
                .post(RequestBody.create(MEDIA_TYPE_TEXT, sc.toString()))
                .build();
        return exeOkHttpRequest(request);
    }
}
