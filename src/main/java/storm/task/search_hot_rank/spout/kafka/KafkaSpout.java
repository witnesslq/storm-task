package storm.task.search_hot_rank.spout.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by mrdops on 7/21/16.
 */
public class KafkaSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private KafkaConsumer<String,String> consumer;
    private String topic;


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
        consumer = createConsumer();
        consumer.subscribe(Arrays.asList(topic));
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record : records) {
                collector.emit(new Values(record.key(),record.value()));
            }
        }
    }

    public void deactivate() {

    }

    public void nextTuple() {

    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hostip","msg"));
    }

    public Map<String, Object> getComponentConfiguration() {
        System.out.println("getComponentConfiguration be called.");
        topic = "com-sohu-mrd-search";
        return null;
    }

    private static KafkaConsumer<String,String> createConsumer(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.10.21.61:9092,10.10.21.62:9092,10.10.21.63:9092,10.10.21.64:9092,10.10.21.65:9092");
        props.put("group.id", "hotkeyword");
        //props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.intervalms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }
}
