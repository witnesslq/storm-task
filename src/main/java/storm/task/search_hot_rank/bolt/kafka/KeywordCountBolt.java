package storm.task.search_hot_rank.bolt.kafka;


import com.sohu.mrd.framework.redis.client.CodisRedisClient;
import com.sohu.mrd.framework.redis.client.codis.JedisResourcePool;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yonghongli on 2016/7/18.
*/
public class KeywordCountBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(KeywordCountBolt.class);

    public static JedisResourcePool jedisPool = null;
    public Jedis jedis=null;
    private static final String clusterName = "mrd_redis_1";
    @Override
    public void prepare(Map stormConf, TopologyContext context ) {

        super.prepare(stormConf, context);
        initialJedisPool();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       // outputFieldsDeclarer.declare(new Fields("keyword", "count"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String word = input.getStringByField("keyword");
        String time = input.getStringByField("dateHour");
        String key = CodisRedisClient.mkKey("hotword_" + time, "search-hot-word");

        try {
            jedis =jedisPool.getResource();
            logger.info("get one message is {}", word);
            logger.info("get one key is {}", key);
            long l =jedis.hincrBy(key, word,1);
            jedis.expire(key, 30*86400);
            logger.info("get return is {}", l);
        }catch(Exception e){
            logger.info("key value is {} " ,key+":"+word);
            logger.info("jedisClient error  : " + e.toString());
        }finally {
            //回收jedis实例
            if (jedis != null){
                jedis.close();
            }
        }



    }

    @Override
    public void cleanup() {
        if (jedis != null){
            jedis.close();
        }
        super.cleanup();
    }

    private static void initialJedisPool()
    {


        JedisPoolConfig config = new JedisPoolConfig();

        config.setMinEvictableIdleTimeMillis(60000);

        config.setTimeBetweenEvictionRunsMillis(30000);


        jedisPool = CodisRedisClient.getJedisPool(clusterName, config, 0,"test");

    }
}