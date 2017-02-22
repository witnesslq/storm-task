package web.service;

import com.sohu.mrd.framework.redis.client.CodisRedisClient;
import com.sohu.mrd.framework.redis.client.codis.JedisResourcePool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import storm.task.util.DateUtils;
import storm.task.util.SimpleTopNTool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yonghongli on 2016/8/3.
 */
public class GetHotWordRedisDate {

    private static JedisResourcePool jedisPool = null;
    private static Jedis jedis=null;
    private static final String clusterName = "mrd_redis_1";
    static {
        JedisPoolConfig config = new JedisPoolConfig();

        config.setMinEvictableIdleTimeMillis(60000);

        config.setTimeBetweenEvictionRunsMillis(30000);
        jedisPool =  CodisRedisClient.getJedisPool(clusterName, config, 0, "test");
        jedis =jedisPool.getResource();
    }

    public static Map<String,Integer> getGetRedisDate(String time,int topN){
        String key = CodisRedisClient.mkKey("hotword_" + time, "search-hot-word");
        Map<String,String> h =jedis.hgetAll(key);
        if(h==null){
            return null;
        }
        SimpleTopNTool utl = new SimpleTopNTool(topN);
        for ( Map.Entry<String,String> s:h.entrySet()){
            utl.addElement(new SimpleTopNTool.SortElement(Integer.valueOf(s.getValue()), s.getKey()));
        }
        Map<String,Integer> result = new LinkedHashMap<String,Integer>();
        for(SimpleTopNTool.SortElement ele : utl.getTopN()){
            result.put(ele.getVal().toString(), (int) ele.getCount());
        }

        return result;

    }

    public static Map<String,Integer> getGetRedisDate(String startTime,String endTime,int topN) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-ddHH");
        Date startDate= sdf.parse(startTime);
        Date endDate= sdf.parse(endTime);
        if(startDate.getTime()>endDate.getTime()){
            return null;
        }else if (startDate.getTime()==endDate.getTime()){
            return getGetRedisDate(startTime,topN);
        }else {
            List<String> dateHourList= DateUtils.getDateTimeList(startTime, endTime);
            Map<String,Integer> all = new HashMap<String,Integer>();
            for (String dateHour:dateHourList){
                Map<String,Integer> h =getGetRedisDate(dateHour, 10);
                if(h==null){
                    continue;
                }
                for (String k:h.keySet()){
                    if(all.get(k)!=null){
                        all.put(k,all.get(k)+h.get(k));
                    }else{
                        all.put(k,Integer.valueOf(h.get(k)));
                    }
                }
            }

            SimpleTopNTool utl = new SimpleTopNTool(topN);
            for ( Map.Entry<String,Integer> s:all.entrySet()){
                utl.addElement(new SimpleTopNTool.SortElement(s.getValue(), s.getKey()));
            }
            Map<String,Integer> result =  new LinkedHashMap<String,Integer>();
            for(SimpleTopNTool.SortElement ele : utl.getTopN()){
                result.put(ele.getVal().toString(), (int) ele.getCount());
            }

            return result;

        }



    }

    public static void main(String[] args) throws ParseException {
        System.out.println(DateUtils.getDateTimeList("2016-08-0409","2016-08-0409"));
      //  Map<String,String> h= GetHotWordRedisDate.getGetRedisDate("2016-08-03 15","2016-08-03 17",10);
       Map<String,Integer> h= GetHotWordRedisDate.getGetRedisDate("2016-08-0918",10);
        System.out.println(h.size());
        for (Map.Entry<String, Integer> kw:h.entrySet()){
            System.out.println(kw.getKey() + ":" + kw.getValue());
        }
    }
}
