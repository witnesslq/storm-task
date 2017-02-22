package storm.task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import storm.task.search_hot_rank.bolt.kafka.KeywordCountBolt;
import storm.task.search_hot_rank.bolt.kafka.LogParseToKeywordBolt;
import storm.task.search_hot_rank.spout.kafka.KafkaSpout;

/**
 * Created by yonghongli on 2016/7/27.
 */
public class SearchHotRankKafkaTopology {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkakeywordspout", new KafkaSpout());
        builder.setBolt("keywordbolt",new LogParseToKeywordBolt(),50).shuffleGrouping("kafkakeywordspout");
        builder.setBolt("keywordtoredisbolt",new KeywordCountBolt(),50).fieldsGrouping("keywordbolt", new Fields("keyword"));
        Config config = new Config();
      //  config.setDebug(false);

        if(args != null && args.length > 0){
            config.setNumWorkers(2);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(SearchHotRankKafkaTopology.class.getSimpleName(),config,builder.createTopology());
            Utils.sleep(60000);
            cluster.killTopology(SearchHotRankKafkaTopology.class.getSimpleName());
            cluster.shutdown();
        }
    }
}
