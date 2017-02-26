package storm.task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import storm.task.inredenv.bolt.RedenvCountBolt;
import storm.task.inredenv.spout.KafkaSpout;

/**
 * Created by yonghongli on 2016/7/27.
 */
public class InRedEnvKafkaTopology_f {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkainredenvspout", new KafkaSpout());
        builder.setBolt("redinenvtoredisbolt",new RedenvCountBolt(),1).shuffleGrouping("kafkainredenvspout");
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
            cluster.submitTopology(InRedEnvKafkaTopology_f.class.getSimpleName(),config,builder.createTopology());
            Utils.sleep(10000000);
            cluster.killTopology(InRedEnvKafkaTopology_f.class.getSimpleName());
            cluster.shutdown();

        }
    }
}
