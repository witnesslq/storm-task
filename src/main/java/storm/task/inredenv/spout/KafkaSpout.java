package storm.task.inredenv.spout;

import com.google.common.base.Function;
import com.sohu.smc.common.kafka.Kafka;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by mrdops on 7/21/16.
 */
public class KafkaSpout implements IRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    private SpoutOutputCollector collector;
    private    Kafka kafka;
    private String topic;
    private BlockingQueue<String> quene;


    public KafkaSpout() {
        super();
    }

    public KafkaSpout(String topic){
        this.topic = topic;
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

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {

    }


    public void activate() {
        kafka = createConsumer();
        quene = new LinkedBlockingQueue<String>();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                kafka.consumeForever(topic, "redenvstatistic_storm2", 20, 50, 1000, new Function<byte[], Boolean>() {
                    @Override
                    public Boolean apply(@Nullable byte[] a) {
                        //       LOG.info("quene8.add a=" + new String(a));
                        try {
                            return quene.offer(new String(a,"utf-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            return false;
                        }
                    }
                });
            }
        });
        t.setDaemon(true);
        t.start();
    }

    public void deactivate() {

    }

    public void nextTuple() {
        while(!quene.isEmpty()){
            String msg  = quene.poll();
            collector.emit(new Values(msg));
             LOG.info("quene8.poll msg=" + msg);
        }
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("msg"));
    }

    public Map<String, Object> getComponentConfiguration() {
        System.out.println("getComponentConfiguration be called.");
        topic = "redenv";
        return null;
    }

    private static Kafka createConsumer(){

        Properties props = new Properties();
        props.put("kafka.zookeeper.connect", "10.13.89.76:2181,10.13.89.77:2181,10.13.89.78:2181");
        props.put("bootstrap.servers", "10.13.89.76:9092,10.13.89.77:9092,10.13.89.78:9092,10.13.89.79:9092,10.13.89.80:9092,10.13.89.81:9092,10.13.89.82:9092,10.13.89.83:9092,10.13.89.84:9092,10.13.89.85:9092,10.13.89.86:9092,10.13.89.87:9092,10.13.89.88:9092,10.13.89.89:9092,10.13.89.90:9092,10.13.89.91:9092");
        props.put("group.id", "redenvstatistic_storm2");
        //props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.intervalms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new Kafka(props);
    }
}
