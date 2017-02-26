package storm.task;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import storm.task.outredenv.bolt.RedenvCountBolt;
import storm.task.outredenv.spout.KafkaSpout;

/**
 * Created by yonghongli on 2016/7/27.
 */
public class OutRedEnvKafkaTopology {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaredenvspout", new KafkaSpout());
        builder.setBolt("redenvtoredisbolt",new RedenvCountBolt(),1).shuffleGrouping("kafkaredenvspout");
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
            cluster.submitTopology(OutRedEnvKafkaTopology.class.getSimpleName(),config,builder.createTopology());
            Utils.sleep(1000000);
            cluster.killTopology(OutRedEnvKafkaTopology.class.getSimpleName());
            cluster.shutdown();
        }
    }
}
