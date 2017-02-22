package storm.task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import storm.task.search.bolt.kafka.LogParseBolt;
import storm.task.search.spout.kafka.KafkaSpout;

/**
 * Created by mrdops on 7/21/16.
 */
public class SearchKafkaTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaspout",new KafkaSpout());
        builder.setBolt("logparsebolt",new LogParseBolt()).shuffleGrouping("kafkaspout");


        Config config = new Config();
        config.setDebug(false);

        if(args != null && args.length > 0){
            config.setNumWorkers(2);
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(SearchKafkaTopology.class.getSimpleName(),config,builder.createTopology());
            Utils.sleep(60000);
            cluster.killTopology(SearchKafkaTopology.class.getSimpleName());
            cluster.shutdown();
        }
    }
}
