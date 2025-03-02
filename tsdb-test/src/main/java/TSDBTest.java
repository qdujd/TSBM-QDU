import cn.edu.ruc.start.TSBM;

public class TSDBTest {
    private static String dataPath = "";
    private static int threadNum = 1;
    private static int cacheLine = 400;
    private static int insertNum = 1;
    public static void main(String[] args) throws Exception {

        dataPath = args[2];
        threadNum = Integer.parseInt(args[3]);
        cacheLine = Integer.parseInt(args[4]);
        insertNum = Integer.parseInt(args[5]);
        boolean loadParam = false, appendParam = false, queryParam = false;
        if ("1".equals(args[1])) {
            loadParam = true;
            appendParam = true;
            queryParam = true;
        } else if ("2".equals(args[1])) {
            loadParam = false;
            appendParam = true;
            queryParam = true;
        } else if ("3".equals(args[1])) {
            loadParam = true;
            appendParam = false;
            queryParam = false;
        } else if ("4".equals(args[1])) {
            loadParam = false;
            appendParam = true;
            queryParam = false;
        } else if ("5".equals(args[1])) {
            loadParam = false;
            appendParam = false;
            queryParam = true;
        } else if ("6".equals(args[1])) {
            loadParam = false;
            appendParam = false;
            queryParam = false;
        }
        
        String className = "";
        String ip = "";
        String port = "";
        String userName = "";
        String passwd = "";
        String dataBaseName = "";
        // Influxdb
        if ("1".equals(args[0])) {
            className = "cn.edu.ruc.InfluxdbAdapter";
            ip = "127.0.0.1";
            port = "8086";
            userName = "root";
            passwd = "root";
            dataBaseName = "Influxdb";
        }
        // Timescaledb
        else if ("2".equals(args[0])) {
            className = "cn.edu.ruc.TimescaledbAdapter";
            ip = "127.0.0.1";
            port = "5432";
            userName = "postgres";
            passwd = "postgres";
            dataBaseName = "Timescaledb";
        }
        // Iotdb
        else if ("3".equals(args[0])) {
            className = "cn.edu.ruc.IotdbAdapter";
            ip = "127.0.0.1";
            port = "6667";
            userName = "root";
            passwd = "root";
            dataBaseName = "Iotdb";
        }
        // Opentsdb
        else if ("4".equals(args[0])) {
            className = "cn.edu.ruc.OpentsdbAdapter";
            ip = "127.0.0.1";
            port = "4242";
            userName = "root";
            passwd = "root";
            dataBaseName = "Opentsdb";
        }
        // Druid
        else if ("5".equals(args[0])) {
            className = "cn.edu.ruc.DruidAdapter";
            ip = "127.0.0.1";
            port = "";
            userName = "root";
            passwd = "root";
            dataBaseName = "Druid";
        }
        //暂时为空
        else if ("6".equals(args[0])) {
            
        }
        // TDengine
        else if ("7".equals(args[0])) {
            className = "cn.edu.ruc.TdengineAdapter2";
            ip = "127.0.0.1";
            port = "6030";
            userName = "root";
            passwd = "taosdata";
            dataBaseName = "TDengine";
        }
        if ("0".equals(args[1])) {
            TSBM.generateData(dataPath); //生成数据
        } else {
            TSBM.startPerformTest(dataPath, className, dataBaseName, ip, port, userName, passwd, insertNum, threadNum, cacheLine, false,
                loadParam, appendParam, queryParam);
        }
    }

}
