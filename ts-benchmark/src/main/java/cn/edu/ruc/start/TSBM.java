package cn.edu.ruc.start;

import cn.edu.ruc.adapter.BaseAdapter;
import cn.edu.ruc.utils.FileUtils;
import cn.edu.ruc.utils.ValueUtils;
import cn.edu.ruc.utils.ResultUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.Date;

/**
 * 入口类 BootStrap
 */
public class TSBM {
    public static final String SEPARATOR = ",";
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final Random RANDOM = new Random();
    private static final long SLEEP_TIME = 200L;
    private static final int MAX_FARM = 256; // 修改
    private static final int MAX_ROWS = 300;
    private static final int MAX_SENSOR = 50;
    private static final int SUM_FARM = 2;
    private static final long IMPORT_START = 1514736000000L;

    public static void main(String[] args) throws Exception {
    }

    // /**
    //  * start to test
    //  *
    //  * @param basePath  the data path that the benchmark in runing generate
    //  * @param className database adapter
    //  * @param ip
    //  * @param port
    //  * @param userName
    //  * @param password
    //  */
    // public static void startPerformTest(String basePath, String className, String ip, String port, String userName,
    //                                     String password) {
    //     startPerformTest(basePath, className, ip, port, userName, password, 1, 1, 400, true, true, true, true);
    // }

    /**
     * start to test
     *
     * @param basePath  the data path that the benchmark in runing generate
     * @param className database adapter
     * @param ip
     * @param port
     * @param userName
     * @param password
     * @param generateParam whether to generate disk data,true means  generate,false is not ;
     * @param loadParam     whether to load data to database,true means  load,false is not ;
     */
    //新增两个flag
    public static void startPerformTest(String basePath, String className, String dataBaseName, String ip, String port, String userName,
                                        String password, int insertNum, int threadNum, int cacheLines, boolean generateParam, boolean loadParam, boolean appendParam, boolean queryParam) {
        BaseAdapter adapter = null;
        try {
            adapter = (BaseAdapter) Class.forName(className).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        adapter.initConnect(ip, port, userName, password);
        String dataPath = basePath + "/data/";
        String resultPath = basePath + "/result/";
        String dbName = dataBaseName;
        String method = "";

        // 获取当前时间戳
        long timestamp = System.currentTimeMillis();
        // 创建SimpleDateFormat对象，设置日期格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
        // 将时间戳转换为Date对象
        Date date = new Date(timestamp);
        // 格式化为易读的字符串
        String formattedDate = sdf.format(date);
        dbName = dbName + "_" + formattedDate;
        if(loadParam || appendParam || queryParam){
            if(loadParam){
                method += "i";
            }
            if(appendParam){
                method += "w";
            }
            if(queryParam){
                method += "r";
            }
        }
        if(method != "" && method.length() > 0){
            dbName = dbName + "_" + method;
        }

        String resultFile = resultPath + dbName + ".txt";
        String resultFile_Load = resultPath + dbName + "_load.txt";
        int maxFarm = MAX_FARM;
        int maxRows = MAX_ROWS;
        writeResult(resultFile, "dataBaseName: "+dataBaseName);
        System.out.println("dataBaseName: "+dataBaseName);
        writeResult(resultFile, "time: "+formattedDate);
        System.out.println("time: "+formattedDate);
        if(method != "" && method.length() > 0){
            writeResult(resultFile, "TEST_METHOD: "+method);
            System.out.println("TEST_METHOD: "+method);
        }
        if(loadParam){
            if(insertNum == 1){
                writeResult(resultFile, "insertNum: 432M");
                System.out.println("insertNum: 432M");
            }else if(insertNum == 2){
                writeResult(resultFile, "insertNum: 2160M");
                System.out.println("insertNum: 2160M");
            }
            writeResult(resultFile, "threadNum: " + threadNum);
            System.out.println("threadNum: " + threadNum);
            writeResult(resultFile, "cacheLines: " + cacheLines);
            System.out.println("cacheLines: " + cacheLines);
        }
        writeResult(resultFile, LINE_SEPARATOR);


        // 1 数据生成
        if (generateParam) {
            System.out.println(">>>>>>>>>>generate data begin " + System.currentTimeMillis() + ">>>>>>>>>>");
            generateDiskData(dataPath, maxFarm, maxRows);
            System.out.println("<<<<<<<<<<generate data finished " + System.currentTimeMillis() + "<<<<<<<");
        } else {
            System.out.println(">>>>>>>>>>generate insert data begin " + System.currentTimeMillis() + ">>>>>>>>>>");
//            generateInsertData(dataPath, maxFarm, maxRows);
            System.out.println("<<<<<<<<<<generate insert data finished " + System.currentTimeMillis() + "<<<<<<<");
        }

        // 2 导入
        if (loadParam) {
            // load
            System.out.println(">>>>>>>>>>load data begin " + System.currentTimeMillis() + ">>>>>>>>>>>>>>");
            long time_end = IMPORT_START + 7 * 24 * 3600 * 1000;
            int farms = 2;
            if (insertNum == 1){
                farms = 2;
            }else if(insertNum == 2){
                farms = 10;
            }
            String importResult;
            importResult = importData(adapter, resultFile_Load, IMPORT_START, time_end, farms, threadNum, cacheLines);
            writeResult(resultFile, "##load\tresult");
            writeResult(resultFile, importResult + "ms");
            writeResult(resultFile, LINE_SEPARATOR);
            System.out.println("<<<<<<<<<<load data finished " + System.currentTimeMillis() + "<<<<<<<<<<<");
            // System.out.println(">>>>>>>>>>load data begin " + System.currentTimeMillis() + ">>>>>>>>>>>>>>");
            // String importResult = importData(adapter, dataPath);
            // System.out.println(importResult);

            // System.out.println("<<<<<<<<<<load data finished " + System.currentTimeMillis() + "<<<<<<<<<<<");
        }

        // 3 append测试
        if (appendParam) {
            System.out.println(">>>>>>>>>>append test begin " + System.currentTimeMillis() + ">>>>>>>>>>>>");
            String appendResult = appendPerform(dataPath, adapter, maxFarm, maxRows);
            System.out.println(appendResult);
            writeResult(resultFile, appendResult);
            System.out.println("<<<<<<<<<<append test finished " + System.currentTimeMillis() + "<<<<<<<<<<<<<<");
        }

        // 4 query测试
        if (queryParam) {
            System.out.println(">>>>>>>>>>query test begin>>>>>>>>>>>>>>");
            String readResult = readPerform(adapter);
            System.out.println(readResult);
            writeResult(resultFile, readResult);
            System.out.println("<<<<<<<<<<query end finished<<<<<<<<<<<<<<");
        }

        if (!(generateParam)) {
            System.out.println("test finished");
            System.out.println("test result in file " + resultFile);
        }
        System.out.println("finished");
        System.exit(0);
    }

    /**
     * the method is used to generate disk data.
     *
     * @param basePath the path of saving data
     */
    public static void generateData(String basePath) {
        String dataPath = basePath + "/data/";
        System.out.println(">>>>>>>>>>generate data begin " + System.currentTimeMillis() + ">>>>>>>>>>");
        generateDiskData(dataPath, MAX_FARM, MAX_ROWS);
        System.out.println("<<<<<<<<<<generate data finished " + System.currentTimeMillis() + "<<<<<<<");
    }
///////////////////////////////////////////////////数据生成

    /**
     * 生成磁盘数据
     *
     * @param basePath 存储数据的路径
     * @param maxFarm  最大farm数
     * @param maxRows  每个farm对应的最大设备数
     */
    private static void generateDiskData(String basePath, int maxFarm, int maxRows) {
        // 1 生成load数据 7天 2个风场 共100个设备，每个设备50个传感器的数据
        long importStart = IMPORT_START;// 2018-01-01 00:00:00
        long importEnd = IMPORT_START + 7 * 24 * 3600 * 1000;
//        long importEnd = importStart + 24* 3600 * 1000;
        for (long start = importStart; start <= importEnd; start += 70000) {
            String path = basePath + "/load/load.data";
            long end = importEnd < start + 70000 ? importEnd : start + 70000;
            int sumFarm = SUM_FARM;// 历史数据风场数
            for (int farmId = 1; farmId <= sumFarm; farmId++) {
                FileUtils.writeLine(path, generateData(start, end, farmId, 50));
//                FileUtils.writeLine(path, generateData(start, end, farmId, 50));
            }

        }
        generateInsertData(basePath, maxFarm, maxRows);
    }

    private static void generateInsertData(String basePath, int maxFarm, int maxRows) {

        long importEnd = IMPORT_START + 7 * 24 * 3600 * 1000;
        // 2 生成 1/2/4/8/16/32/64 farm数据 每个farm50个device，每个10批次，一个批次一个文件
        int batchSum = 5;
        long insertStart = importEnd;
        for (int farmNum = 1; farmNum <= maxFarm; farmNum = farmNum * 2) {
            int deviceNum = 50;
            for (int batchNum = 1; batchNum <= batchSum; batchNum++) {
                for (int cFarm = 1; cFarm <= farmNum; cFarm++) {
                    String path = basePath + "/farm/" + farmNum + "/" + batchNum + "/" + cFarm;
                    FileUtils.writeLine(path, generateData(insertStart + 7000 * (batchNum - 1),
                            insertStart + 7000 * batchNum, cFarm, 50));
                }
            }
            insertStart += 7000 * batchSum * farmNum;
        }
        // 3 生成 8个farm，farm50，100，150，200，250，300数据
        for (int rowNum = 50; rowNum <= maxRows; rowNum = rowNum + 50) {
            int farmNum = 8;
            for (int batchNum = 1; batchNum <= batchSum; batchNum++) {
                for (int cFarm = 1; cFarm <= farmNum; cFarm++) {
                    String path = basePath + "/device/" + rowNum + "/" + batchNum + "/" + cFarm;
                    FileUtils.writeLine(path, generateData(insertStart + 7000 * (batchNum - 1)
                            , insertStart + 7000 * batchNum, cFarm, rowNum));
                }
            }
            insertStart += 7000 * batchSum * farmNum;
        }
    }

    private static String generateData(long start, long end, long farmId, long rows) {
        int step = 7000;
        int sumSensor = MAX_SENSOR;
        StringBuffer dataBuffer = new StringBuffer();
        for (; start < end; start += step) {
            for (int rowIndex = 1; rowIndex <= rows; rowIndex++) {
                StringBuffer dBuffer = new StringBuffer();
                dBuffer.append(start);
                dBuffer.append(SEPARATOR);
                dBuffer.append("f" + farmId);
                dBuffer.append(SEPARATOR);
                dBuffer.append("d" + rowIndex);
                for (int sn = 1; sn <= sumSensor; sn++) {
                    dBuffer.append(SEPARATOR);
//                    dBuffer.append(String.format("%.5f", RANDOM.nextDouble() * sn));
                    dBuffer.append(String.format("%.5f", ValueUtils.getValueByField((int) farmId, sn, start)));
                }
                dBuffer.append(LINE_SEPARATOR);
                dataBuffer.append(dBuffer.toString());
            }
        }
        return dataBuffer.toString();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////数据导入////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static String importData(BaseAdapter adapter, String dataPath) {
        String path = dataPath + "/load/load.data";
        int cacheLine = 3000;
        StringBuffer data = new StringBuffer();
        try {
            FileReader fr = new FileReader(path);
            BufferedReader bf = new BufferedReader(fr);
            String str = "";
            int count = 1;
            // 按行读取字符串
            while ((str = bf.readLine()) != null) {
                data.append(str);
                data.append(System.getProperty("line.separator"));
                if (count % cacheLine == 0) {//每1000条插入一次
                    try {
                        long timeout = adapter.insertData(data.toString());
                        long pps = 0;
                        if (timeout > 0) {
                            pps = (cacheLine * 50 * 1000 / timeout);
                        }
                        System.out.println("import pps " + pps + " points/s");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    data.setLength(0);
                }
                count++;
            }
            if (data.length() != 0) {
                adapter.insertData(data.toString());
            }
            bf.close();
            fr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private static String importData(BaseAdapter adapter, String path,long time_start, long time_end, int farms, int threadNum, int cacheLines)
    {
        StringBuffer[] data = new StringBuffer[threadNum];
        for (int i = 0;i < threadNum;i++){
            data[i] = new StringBuffer();
        }

        int cacheLine = cacheLines; //每单位代表50行
        int cacheThread = cacheLine / threadNum;
        // cacheThread++; // 防止小数
        int index = 0;
        // long interval = 70000;
        int count = 0;
        // time_start += 589540000L;
        int count_L = 0; 
        long Time_sum = 0L;
        ExecutorService executor = Executors.newFixedThreadPool(threadNum+1); // 创建一个固定大小为 threadNum 的线程池
        CompletionService<Long> cs = new ExecutorCompletionService<>(executor); // 初始化一个 CompletionService，并将 executor 线程池与之绑定。
        FileUtils.writeLine(path, "pps(points/s),timeout(ms)");
        for (long start = time_start; start < time_end; start += 7000) {
            // long end = time_end < start + 70000 ? time_end : start + 70000;
            for (int farmId = 1; farmId <= farms; farmId++) {
                int sumSensor = MAX_SENSOR;
                // StringBuffer dataBuffer = new StringBuffer();
                for (int rowIndex = 1; rowIndex <= 50; rowIndex++) {
                    data[index].append(start);
                    data[index].append(SEPARATOR);
                    data[index].append("f" + farmId);
                    data[index].append(SEPARATOR);
                    data[index].append("d" + rowIndex);
                    for (int sn = 1; sn <= sumSensor; sn++) {
                        data[index].append(SEPARATOR);
                        try{
                            data[index].append(String.format("%.5f", ValueUtils.getValueByField((int) farmId, sn, start)));
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("already load:"+count_L);
                        }
                        
                    }
                    data[index].append(LINE_SEPARATOR);
                }
                count++;
                count_L++;
                if(count == cacheThread*(index+1)){
                    index++;
                }
            }
            if (count == cacheLine) {//每 20000 行数据(100w个)插入一次
                try {
                        try {
                            long st_time = System.currentTimeMillis();
                            for(int i = 0;i < threadNum;i++){
                                final int index1 = i;
                                // if(data[index1].length() != 0){
                                // System.out.println(data[index1].length());
                                cs.submit(new Callable<Long>() {
                                    public Long call() throws Exception {
                                        return adapter.insertData(data[index1].toString());
                                    }
                                });
                                // }
                            }
                            long timeout = 0;
                            for(int i = 0;i < threadNum;i++){
                                long t = cs.take().get();
                                // System.out.println(t);
                                timeout = timeout + t;
                            }
                            long ed_time = System.currentTimeMillis();
                            timeout = ed_time - st_time;
                            for(int i = 0;i < threadNum;i++){
                                data[i].setLength(0);
                                // 先获取完时间开销，再重置为0
                                // 防止对异步操作造成影响
                            }
                            long pps = 0;
                            Time_sum += timeout;
                            if (timeout > 0) {
                                pps = (count * 50 * 50 * 1000 / timeout);
                            }
                            if(pps == 0){
                                System.out.println(count);
                                System.out.println(timeout);
                            }
                            index = 0;
                            count = 0;
                            System.out.println("import pps " + pps + " points/s");
                            FileUtils.writeLine(path, String.valueOf(pps)+','+String.valueOf(timeout));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        FileUtils.writeLine(path, String.valueOf(Time_sum));
        return String.valueOf(Time_sum);
    }

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////写入测试/////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * append test
     */
    private static String appendPerform(String basePath, BaseAdapter adapter, int maxFarm, int maxRows) {
        int sleepTime = 7000;
        StringBuffer appendResultBuffer = new StringBuffer();
        System.out.println(">>>>>>>>>>append-1 start " + System.currentTimeMillis() + ">>>>>>>>>>");
        appendResultBuffer.append("##append\tresult");
        appendResultBuffer.append(LINE_SEPARATOR);
        // farm++ test
        appendResultBuffer.append("###append\tfarm++\tresult");
        appendResultBuffer.append(LINE_SEPARATOR);
        for (int farm = 1; farm <= maxFarm; farm = farm * 2) {
            int batchMax = 5;
            int row = 50;
            ExecutorService pool = Executors.newFixedThreadPool(farm);
            CompletionService<Long> cs = new ExecutorCompletionService<Long>(pool);
            long sumPps = 0L;
            //每个风场，每个7s发送一次数据
            Map<Integer, Integer> thinkTimeMap = new HashMap<Integer, Integer>();
            for (int cFarm = 1; cFarm <= farm; cFarm++) {
                int thinkTime = RANDOM.nextInt(sleepTime);
                thinkTimeMap.put(cFarm, thinkTime);
            }
            for (int batch = 1; batch <= batchMax; batch++) {
                long startTime = System.currentTimeMillis();
                for (int cFarm = 1; cFarm <= farm; cFarm++) {
                    String path = basePath + "/farm/" + farm + "/" + batch + "/" + cFarm;
                    executeAppend(adapter, cs, path, thinkTimeMap.get(cFarm));
                }
                long pps = calcThroughtPut(row, farm, cs);
                sumPps += pps;
                System.out.println("append 1." + farm + "." + batch + " finished " + pps);
                long endTime = System.currentTimeMillis();
                long costTime = endTime - startTime;
                // 每七秒执行一次
                sleep(sleepTime - costTime > 0 ? sleepTime - costTime : 1);
            }
            appendResultBuffer.append("farm");
            appendResultBuffer.append("\t\t");
            appendResultBuffer.append(farm);
            appendResultBuffer.append("\t\t");
            appendResultBuffer.append(sumPps / batchMax);
            appendResultBuffer.append(LINE_SEPARATOR);
            pool.shutdown();
        }
        System.out.println(">>>>>>>>>>append-1 end " + System.currentTimeMillis() + ">>>>>>>>>>");
        System.out.println(">>>>>>>>>>append-2 start " + System.currentTimeMillis() + ">>>>>>>>>>");
        // row++ test
        appendResultBuffer.append("###append\tdevice++\tresult");
        appendResultBuffer.append(LINE_SEPARATOR);
        for (int row = 50; row <= maxRows; row = row + 50) {
            int batchMax = 5;
            int farm = 8;//线程数
            ExecutorService pool = Executors.newFixedThreadPool(farm);
            CompletionService<Long> cs = new ExecutorCompletionService<Long>(pool);
            long sumPps = 0L;
            //每个风场，每个7s发送一次数据
            Map<Integer, Integer> thinkTimeMap = new HashMap<Integer, Integer>();
            for (int cFarm = 1; cFarm <= farm; cFarm++) {
                int thinkTime = RANDOM.nextInt(sleepTime);
                thinkTimeMap.put(cFarm, thinkTime);
            }
            for (int batch = 1; batch <= batchMax; batch++) {
                long startTime = System.currentTimeMillis();
                for (int cFarm = 1; cFarm <= farm; cFarm++) {
                    String path = basePath + "/device/" + row + "/" + batch + "/" + cFarm;
                    executeAppend(adapter, cs, path, thinkTimeMap.get(cFarm));
                }
                long pps = calcThroughtPut(row, farm, cs);
                sumPps += pps;
                System.out.println("append 2." + row + "." + batch + " finished " + pps);
                long endTime = System.currentTimeMillis();
                long costTime = endTime - startTime;
                // 每七秒执行一次
                sleep(sleepTime - costTime > 0 ? sleepTime - costTime : 1);
            }
            appendResultBuffer.append("device");
            appendResultBuffer.append("\t\t");
            appendResultBuffer.append(row);
            appendResultBuffer.append("\t\t\t");
            appendResultBuffer.append(sumPps / batchMax);
            appendResultBuffer.append(LINE_SEPARATOR);
            pool.shutdown();
        }
        System.out.println(">>>>>>>>>>append-2 end " + System.currentTimeMillis() + ">>>>>>>>>>");
        return appendResultBuffer.toString();
    }

    /**
     * 计算append吞吐量
     * 计算吞吐量 throughtput= sum(points)/avg(timeout) ||sum(rows)/avg(timeout)
     *
     * @param row  每个farm的设备数
     * @param farm 风场数
     * @param cs   任务
     */
    private static long calcThroughtPut(int row, int farm, CompletionService<Long> cs) {
        long points = farm * row * MAX_SENSOR;//50个设备，50个传感器
        double avgTime = calAvgTimeout(farm, cs);
        long pps = 0L;
        if (avgTime == 0) {
            pps = 0L;
        } else {
            pps = (long) (points / avgTime);
        }
        return pps;
    }

    /**
     * 调用适配器，执行写入操作
     *
     * @param adapter 适配器
     * @param cs      执行线程
     * @param path    数据来源路径
     */
    private static void executeAppend(BaseAdapter adapter, CompletionService<Long> cs, String path) {
        cs.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                String data = FileUtils.read(path);
                return adapter.insertData(data);
            }
        });
    }

    private static void executeAppend(BaseAdapter adapter, CompletionService<Long> cs, String path, final int sleepTime) {
        cs.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                String data = FileUtils.read(path);
                Thread.currentThread().sleep(sleepTime);
                return adapter.insertData(data);
            }
        });
    }

    /**
     * 计算响应时间
     *
     * @param farm
     * @param cs
     * @return avg timeout 单位为s
     */
    private static double calAvgTimeout(int farm, CompletionService<Long> cs) {
        double sumTimeout = 0;
        int successTime = 0;
        try {
            for (int index = 1; index <= farm; index++) {
                Long timeout = cs.take().get();
                if (timeout > 0) {
                    sumTimeout += timeout;
                    successTime++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (successTime == 0) {
            return 0;
        } else {
            return sumTimeout / successTime / 1000.0;
        }

    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////read 测试
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private static String readPerform(BaseAdapter adapter) {
        StringBuffer resultBuffer = new StringBuffer();
        resultBuffer.append("##query\tresult");
        resultBuffer.append(LINE_SEPARATOR);
        int batch = 10;


        // 五类查询，每类10个批次
        long time = 1514736000000l;//2018-01-01 00:00:00
       //long time = 1514822400000l;//test
        // 首先查询一次刷新数据
        adapter.query1(time, time + 100 * 3600 * 24);
        sleep(SLEEP_TIME);
        System.out.println(">>>>>>>>>>query-1 end " + System.currentTimeMillis() + ">>>>>>>>>>");
        // 每一批次比前一批次时间维度平移1hour
        long slipUnit = 3600 * 1000;
        long sumTimeout1 = 0;
        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;//1 hour
            long incre = 3600 * 1000;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query1(start, end);
            sumTimeout1 += timeout;
            System.out.println("query 1." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query1");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout1 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        System.out.println(">>>>>>>>>>query-2 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        long sumTimeout2 = 0;
        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;//1 day
            long incre = 3600 * 1000 * 24;
            long end = time + incre + (cBatch - 1) * slipUnit;
            double value = 3.0;//阈值 TODO
            long timeout = adapter.query2(start, end, value);
            sumTimeout2 += timeout;
            System.out.println("query 2." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query2");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout2 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        long sumTimeout3 = 0;
        System.out.println(">>>>>>>>>>query-3 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;// 1 day
            long incre = 3600 * 1000 * 24;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query3(start, end);
            sumTimeout3 += timeout;
            System.out.println("query 3." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query3");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout3 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        long sumTimeout4 = 0;
        System.out.println(">>>>>>>>>>query-4 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;// 15 min
            long incre = 1000 * 15 * 50;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query4(start, end);
            sumTimeout4 += timeout;
            System.out.println("query 4." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query4");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout4 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        System.out.println(">>>>>>>>>>query-5 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        long sumTimeout5 = 0;
        for (int cBatch = 1; cBatch <= batch; cBatch++) {
            long start = time + (cBatch - 1) * slipUnit;//15 min
            long incre = 1000 * 15 * 50;
            long end = time + incre + (cBatch - 1) * slipUnit;
            long timeout = adapter.query5(start, end);
            sumTimeout5 += timeout;
            System.out.println("query 5." + cBatch + " finished " + timeout);
            sleep(SLEEP_TIME);
        }
        resultBuffer.append("query5");
        resultBuffer.append("\t");
        resultBuffer.append(sumTimeout5 / batch);
        resultBuffer.append(LINE_SEPARATOR);
        // System.out.println(">>>>>>>>>>query-6 end " + System.currentTimeMillis() + ">>>>>>>>>>");

        // ResultUtils query6 = adapter.query6();
        // System.out.println("query 6" + " finished " + query6.getTimeout() + ", and count(*) is "+ query6.getValue());
        // sleep(SLEEP_TIME);
        // resultBuffer.append("query6");
        // resultBuffer.append("\t");
        // resultBuffer.append(query6.getTimeout());
        // resultBuffer.append("\t");
        // resultBuffer.append(query6.getValue());
        // resultBuffer.append(LINE_SEPARATOR);
        return resultBuffer.toString();
    }

    private static void sleep(long sleepTime) {
        try {
            Thread.currentThread().sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////输出结果////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    private static void writeResult(String resultPath, String data) {
        FileUtils.writeLine(resultPath, data);
    }
}
