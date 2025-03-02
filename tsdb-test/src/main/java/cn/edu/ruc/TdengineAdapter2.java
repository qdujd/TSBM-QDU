package cn.edu.ruc;
import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.start.TSBM;
import cn.edu.ruc.utils.ResultUtils;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;

import org.json.zip.None;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TdengineAdapter2 implements BaseAdapter {

    private Connection connection = null;
    private String url = null;
    private String ip = null;
    private String port = null;

    public Connection getConnection() {
        if (connection != null) {
            return connection;
        }
        this.url = String.format("jdbc:TAOS://%s:%s?user=root&password=taosdata", ip, port);
        try {
            // Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties connProps = new Properties();
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection(url, connProps);
            if (connection == null) {
                return null;
            }
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            connection = null;
            return null;
        }
    }

    @Override
    public void initConnect(String ip, String port, String user, String password) {
        this.ip = ip;
        this.port = port;
        connection = getConnection();
        // 创建数据库
        try {
            Statement stm = connection.createStatement();
            stm.executeUpdate("create database if not exists test;");
            stm.executeUpdate("use test;");
            // // 创建超级表
            stm.executeUpdate("create stable metrics (ts timestamp, valuess float) TAGS (farm nchar(6), device nchar(6), s nchar(4));");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return;
    }

    @Override
    public long insertData(String data) {
        String[] rows = data.split(TSBM.LINE_SEPARATOR); // 分割行
        StringBuffer sqls = new StringBuffer();
        String sqlFormat = "(\"%s\",%s,%s,\"%s\",\"%s\",\"%s\")";
        String sql_head = "INSERT INTO metrics (tbname, ts, valuess, farm, device, s) VALUES ";
        // String sqlFormat = "%s USING metrics TAGS (\"%s\",\"%s\",\"%s\") VALUES (%s) "; // 更改了插入SQL语句格式
        long costTime = 0L;
        for (String row : rows) {
            String[] sensors = row.split(TSBM.SEPARATOR); // 分割列
            if (sensors.length < 3) { // 过滤空行
                continue;
            }
            String timestamp = sensors[0];
            String farmId = sensors[1];
            String deviceId = sensors[2];
            // StringBuffer values = new StringBuffer();
            int length = sensors.length;
            // System.out.println(length);
            StringBuffer sql = new StringBuffer();
            for (int index = 3; index < length; index++) {
                String sensorName = "s" + (index - 2);
                String value = sensors[index];
                String tbname = farmId + deviceId + sensorName;
                sql.append(String.format(sqlFormat, tbname, timestamp, value, farmId, deviceId, sensorName)); // 使用批量插入格式
                sql.append(" ");
                // values.setLength(0);
            }
            if (sqls.length() + sql.length() + sql_head.length() > 1048576) {
                String SQL = sql_head + sqls.toString();
                sqls = sql;
                long startTime = System.nanoTime();
                connection = getConnection();
                if (connection != null) {
                    try {
                        Statement stmt = connection.createStatement();
                        // System.out.println(SQL);
                        try {
                            stmt.execute(SQL); // 执行批量插入
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        // sqls.setLength(0);
                        stmt.close();
                        long endTime = System.nanoTime();
                        costTime += (endTime - startTime) / 1000 / 1000;
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            } else {
                sqls.append(sql);
                sql.setLength(0);
            }
        }
        if (sqls.length() != 0) { // 执行剩余数据的插入
            String SQL = sql_head + sqls.toString();
            if (connection != null) {
                try {
                    long startTime = System.nanoTime();
                    Statement stmt = connection.createStatement();
                    try {
                        stmt.execute(SQL); 
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    stmt.close();
                    long endTime = System.nanoTime();
                    costTime += (endTime - startTime) / 1000 / 1000;
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        return costTime;
    }

    private void closeStatement(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long execQuery(String sql) {
        connection = getConnection();
        Statement statement = null;
        long costTime = 0;
        try {
            statement = connection.createStatement();
            long startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        } finally {
            closeStatement(statement);
        }
        return costTime / 1000 / 1000;
    }

    public ResultUtils execQuery1(String sql) {
        connection = getConnection();
        Statement statement = null;
        long costTime = 0;
        long result = 0;
        try {
            statement = connection.createStatement();
            long startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            result = rs.getLong("count(*)");
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
        } catch (SQLException e) {
            e.printStackTrace();
            return new ResultUtils(0L,0L);
        } finally {
            closeStatement(statement);
        }
        costTime = costTime / 1000 / 1000;
        return new ResultUtils(result, costTime);
    }

    @Override
    public long query1(long start, long end) {
        String sqlFormat = "select * from metrics where farm = '%s' and device = '%s' and s = '%s' and ts >= %s and ts <= %s";
        String eSql = String.format(sqlFormat, "f1", "d2", "s3", start, end);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
    }

    @Override
    public long query2(long start, long end, double value) {
        String sqlFormat = "select * from metrics where farm = '%s' and s = '%s' and valuess >= %s and ts >= %s and ts < %s";
        String eSql = String.format(sqlFormat, "f1","s2",value, start, end);
        System.out.println(eSql);
        return execQuery(eSql);
        //return 1;
    }

    @Override
    public long query3(long start, long end) {
        String sqlFormat = "SELECT farm,device,s,TIMETRUNCATE(ts,1h) as time_bucket,AVG(valuess) AS avg_value FROM metrics WHERE farm = '%s' AND s = '%s' AND ts >= %d AND ts <= %d GROUP BY device,farm,s,TIMETRUNCATE(ts,1h);";
        String eSql = String.format(sqlFormat, "f1", "s1", start, end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    @Override
    public long query4(long start, long end) {
        String sqlFormat = "select * from metrics where farm = '%s' and (s = '%s' or s = '%s' or s = '%s' or s = '%s' or s = '%s') " +
                "and ts >= %s and ts <= %s";
        String eSql = String.format(sqlFormat,"f1","s1", "s2", "s3", "s4", "s5", start, end);
        System.out.println(eSql);
        return execQuery(eSql);
    }

    @Override
    public long query5(long start, long end) {
        String sqlFormat = "select * from metrics where farm = '%s' and ts >= %s and ts <= %s";
        String eSql = String.format(sqlFormat, "f1", start, end);
        System.out.println(eSql);
        return execQuery(eSql);
    }
    @Override
    public ResultUtils query6() {
        String eSql = "select count(*) from metrics";
        System.out.println(eSql);
        return execQuery1(eSql);
    }
}
