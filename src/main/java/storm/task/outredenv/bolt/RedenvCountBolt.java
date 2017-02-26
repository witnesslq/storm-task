package storm.task.outredenv.bolt;

import net.sf.json.JSONObject;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

/**
 * Created by yonghongli on 2016/7/18.
 */
public class RedenvCountBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(RedenvCountBolt.class);

    public static JedisPool jedisPool = null;
    public Jedis jedis = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        super.prepare(stormConf, context);
        initialJedisPool();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // outputFieldsDeclarer.declare(new Fields("keyword", "count"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String redenvstr = input.getString(0);
        logger.info("rev ====================================:" + redenvstr);
        JSONObject object = JSONObject.fromObject(redenvstr);
        String batchId = object.getString("batchId");
        try {
            //  Double money = new Double(object.getString("money"));
            Double money = Double.parseDouble(object.getString("money"));
            String key = batchId;

            jedis = jedisPool.getResource();
            long n = jedis.hincrBy(key, "num", 1);
            long m = jedis.hincrBy(key, "money", (long) (money * 100));
            jedis.expire(key, 30 * 86400);

        } catch (Exception e) {
            logger.info("[execute] convert to double exception", e);
        } finally {
            //回收jedis实例
            if (jedis != null) {
                jedis.close();
            }
        }


    }

    @Override
    public void cleanup() {
        if (jedis != null) {
            jedis.close();
        }
        super.cleanup();
    }

    private static void initialJedisPool() {


        JedisPoolConfig config = new JedisPoolConfig();

        config.setMinEvictableIdleTimeMillis(60000);

        config.setTimeBetweenEvictionRunsMillis(30000);


        jedisPool = new JedisPool(config, "10.10.93.181", 6100, 20000);

    }
}