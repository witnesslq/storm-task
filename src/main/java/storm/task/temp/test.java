package storm.task.temp;

import com.sohu.mrd.framework.redis.client.CodisRedisClient;
import com.sohu.mrd.framework.redis.client.codis.JedisResourcePool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.Set;

/**
 * Created by yonghongli on 2016/8/3.
 */
public class test {
    public static void main(String[] args){
        JedisResourcePool jedisPool = null;
        Jedis jedis=null;
         String clusterName = "mrd_redis_1";
        JedisPoolConfig config = new JedisPoolConfig();

        config.setMinEvictableIdleTimeMillis(60000);

        config.setTimeBetweenEvictionRunsMillis(30000);
        jedisPool =  CodisRedisClient.getJedisPool(clusterName, config, 0, "test");
        jedis =jedisPool.getResource();
        String key = CodisRedisClient.mkKey("hotword_" +"2016-08-1119", "search-hot-word");
       // jedis.del(key);

        Map<String,String> h =jedis.hgetAll(key);
        System.out.println(h.size());
//        Set<String> s =jedis.keys("*");
//        for (String ss:s){
//            System.out.println(ss);
//        }
        for (Map.Entry<String, String> kw:h.entrySet()){
            System.out.println(kw.getKey() + ":" + kw.getValue());
        }

    }
}
