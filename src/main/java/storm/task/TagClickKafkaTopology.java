package storm.task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import storm.task.tag_click.bolt.kafka.TagClickDataToESBolt;
import storm.task.tag_click.spout.kafka.KafkaSpout;

//import org.apache.storm.spout.SchemeAsMultiScheme;
//import storm.task.tag.bolt.kafka.ReportTagBolt;
//import storm.task.util.ConfigUtil;
//import java.util.Properties;

/**
 * Created by yonghongli on 2016/7/18.
 */
public class TagClickKafkaTopology {
    private static final String KAFKA_SPOUT_ID = "kafkaSpout";
    private static final String DATATOES_BOLT_ID = "dateToESBolt";
    private static final String REPORT_BOLT_ID ="report_log_id";
    private static final String CONSUME_TOPIC = "com-sohu-mrd-nr";
    private static final String ZK_ROOT = "/storm";
    private static final String ZK_ID = "tagToES";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout());

        builder.setBolt(DATATOES_BOLT_ID, new TagClickDataToESBolt(),20).shuffleGrouping(KAFKA_SPOUT_ID);

        Config config = new Config();
        config.setDebug(false);
        if(args != null && args.length > 0){
            config.setNumWorkers(2);
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TagClickKafkaTopology.class.getSimpleName(),config,builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology(TagClickKafkaTopology.class.getSimpleName());
            cluster.shutdown();
        }

    }
}