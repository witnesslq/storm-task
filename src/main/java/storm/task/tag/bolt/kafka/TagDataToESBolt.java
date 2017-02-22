package storm.task.tag.bolt.kafka;

import net.sf.json.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.task.operation.StoreIndex;

import java.util.*;


/**
 * Created by yonghongli on 2016/7/18.
 */
public class TagDataToESBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(TagDataToESBolt.class);
    private OutputCollector collector;


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String ip =  tuple.getStringByField("hostip");
        String line = tuple.getStringByField("msg");
        logger.info("get one message is {}", line);
        List<JSONObject> tagList = parse(line, ip);
        ObjectMapper mapper = new ObjectMapper();
        String day = null;
        if (tagList != null) {
            try {
                Map<String,Object> productMap = mapper.readValue(tagList.get(0).toString(),Map.class);
                day =  productMap.get("@timestamp").toString().split("T")[0];

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if(tagList!=null&&day!=null){
            for (JSONObject obj :tagList){
                //System.out.println(obj.size()+":"+obj.toString());
                StoreIndex.storeToES("com-sohu-mrd-tag", day, obj.toString());
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      //  outputFieldsDeclarer.declare(new Fields("stream","streamid","cid","oid","nid","nidtype","@timestamp"));
    }

    private static List<JSONObject> parse(String line,String ip){
        List<JSONObject> result =null;
        try {
        result = new ArrayList<JSONObject>();
        String[] st1= line.split("news:\\[");
        String time = st1[0].split("]")[0].split("\\[")[1];
            time = (time+"+08:00").replace(" ","T");
        String stream = st1[0].split(",")[0].split(":")[3];
        String streamid = st1[0].split(",")[1].split(":")[1];
        String cid = st1[0].split(",")[2].split(":")[1];
        String[] news = st1[1].split("], adInfo:");
        String[] o_n_ot_sts = news[0].split(",");
        for(String o_n_ot_st:o_n_ot_sts){
            HashMap<String,String> hm = new HashMap<>();
            hm.put("ip",ip);
            hm.put("@timestamp",time);
            hm.put("stream",stream);
            hm.put("streamid",streamid);
            hm.put("cid",cid);
            String[] onn= o_n_ot_st.split("_");
            String oid = onn[0];
            String nid = onn[1];
            //    String oidtype = onn[2];
            String nidtype = onn[3];
            hm.put("oid",oid);
            hm.put("nid",nid);
            hm.put("nidtype",nidtype);
            JSONObject jsonData = JSONObject.fromObject(hm);
            result.add(jsonData);
        }
        }catch (Exception e){
            result=null;
        }
       return result;
    }

}