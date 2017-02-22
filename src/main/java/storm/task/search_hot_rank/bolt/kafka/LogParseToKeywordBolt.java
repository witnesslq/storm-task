package storm.task.search_hot_rank.bolt.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.task.operation.StoreIndex;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by mrdops on 7/21/16.
 */
public class LogParseToKeywordBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(LogParseToKeywordBolt.class);
    //define log format
    String ts_re = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}).*";
    String ln_re = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}),\\d{3} .*? (\\w{4}) - (.*?) - .*?request param:(.*?),响应时间:(.*?)ms,.*?(\\{.*\\})";
    Pattern p_ts_re = Pattern.compile(ts_re);
    Pattern p_ln_re = Pattern.compile(ln_re);
    HashSet<String>  cids = new HashSet<>();
    public LogParseToKeywordBolt() {
        super();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("keyword","dateHour"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
          String url = "jdbc:mysql://10.13.85.168:3306/media_from_smc";
          String name = "com.mysql.jdbc.Driver";
          String user = "developer";
          String password = "123456";
        String sql = "select * from search_black_cid";
          Connection conn = null;
          PreparedStatement pst = null;
        try {
            Class.forName(name);//指定连接类型
            conn = DriverManager.getConnection(url, user, password);//获取连接
            pst = conn.prepareStatement(sql);//准备执行语句
            ResultSet resultSet = pst.executeQuery();

                while (resultSet.next()) {
                    String cid = resultSet.getString(2);
                    if(!cid.trim().equals("")){
                        cids.add(cid.trim());
                    }

                }//显示数据
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                pst.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String newline = (String) tuple.getValue(1);
        Matcher m = p_ts_re.matcher(newline);

        String[] request;
        Map<String, Object> reqMap = new HashMap<String, Object>();

        if (m.find()&&newline.contains("响应时间")){

                
                Matcher n = p_ln_re.matcher(newline);
               if(n.find()) {

                   try {
                       String timestamp = n.group(1);
                       String dateHour= timestamp.split(":")[0].replace(" ","");
                       request = n.group(4).split(",");
                       for (int i = 0; i < request.length; i++) {
                           String[] item = request[i].split("=");
                           String key = item[0];
                           String value = item[1].substring(1, item[1].length() - 1);
                           reqMap.put(key, value);
                       }
                       Object cid = reqMap.get("cid");
                       if ((cid!=null)&&cids.contains(cid.toString().trim())) {
                           return;
                       }
                       if(cid==null){
                           return;
                       }
                       Object kw= reqMap.get("keywords");
                       if (kw!=null){
                           String keyword = kw.toString();
                           basicOutputCollector.emit(new Values(keyword,dateHour));
                       }

                   } catch (Exception e) {
                       e.printStackTrace();
                   }
               }
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
