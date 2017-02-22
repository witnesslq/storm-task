package storm.task.temp;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import storm.task.tag.bolt.kafka.TagDataToESBolt;
import storm.task.util.ConfigUtil;

import java.util.Properties;

/**
 * Created by yonghongli on 2016/7/18.
 */
public class TagKafkaTopology {
    private static final String KAFKA_SPOUT_ID = "kafkaSpout";
    private static final String DATATOES_BOLT_ID = "dateToESBolt";
    private static final String REPORT_BOLT_ID ="report_log_id";
    private static final String CONSUME_TOPIC = "com-sohu-mrd-nr-189";
    private static final String ZK_ROOT = "/storm";
    private static final String ZK_ID = "tagToES";

    public static void main(String[] args) throws Exception {
        // 配置Zookeeper地址
        Properties prop = ConfigUtil.getKafkaProperties();
        BrokerHosts brokerHosts=  new ZkHosts(prop.getProperty("kafka.zookeeper.connect"));
        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, CONSUME_TOPIC, ZK_ROOT, ZK_ID);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout(spoutConfig));
     //   builder.setBolt(REPORT_BOLT_ID, new ReportTagBolt()).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(DATATOES_BOLT_ID, new TagDataToESBolt()).shuffleGrouping(KAFKA_SPOUT_ID);

        Config config = new Config();

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("test", config, builder.createTopology());
//        Utils.sleep(100000);
//        cluster.killTopology("test");
//        cluster.shutdown();
        config.setNumWorkers(5);
        StormSubmitter.submitTopology(TagKafkaTopology.class.getSimpleName(), config, builder.createTopology());

    }
}