package storm.task.temp.bolt.kafka;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yonghongli on 2016/7/18.
*/
public class WordCountBolt extends BaseBasicBolt {
    private Map<String, Long> counts = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counts = new ConcurrentHashMap<>();
        super.prepare(stormConf, context);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");
        Long count = this.counts.get(word);
        if (count == null) {
            count = 0L;
        }
        count++;
        this.counts.put(word, count);
        collector.emit(new Values(word, count));
    }
}