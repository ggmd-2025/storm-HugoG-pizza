package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class ExitInLogBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger("ExitInLogBolt");
    private OutputCollector collector;

    public ExitInLogBolt() {}

    @Override
    public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        // On n'affiche pas tout pour préserver les logs, ou juste un petit marqueur
        // String n = t.getValueByField("json").toString();
        // logger.info("[ExitInLOG] Tuple traité"); 
        
        collector.ack(t);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {}
}