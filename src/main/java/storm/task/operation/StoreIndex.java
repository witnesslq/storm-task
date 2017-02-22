package storm.task.operation;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.settings.Settings;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by yonghongli on 2016/7/26.
 */
public class StoreIndex implements Serializable {


    public  static TransportClient client;

    public static synchronized TransportClient getClient(){

        if(client !=null){
            return client;
        }

        Settings settings = Settings.settingsBuilder().put("cluster.name", "mrd-es-platform").put("client.transport.sniff", true).build();

        try {
            client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.21.56"), 9302)).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.10.21.57"), 9302));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return client;
    }


    public static void storeToES(String index,String type,String id,String source){
        TransportClient client = StoreIndex.getClient();
        IndexResponse response = client.prepareIndex(index,type,id).setSource(source).get();
    }
    public  static  void storeToES(String index,String type,String source){
        TransportClient client = StoreIndex.getClient();
        IndexResponse response = client.prepareIndex(index,type).setSource(source).get();
    }

}
