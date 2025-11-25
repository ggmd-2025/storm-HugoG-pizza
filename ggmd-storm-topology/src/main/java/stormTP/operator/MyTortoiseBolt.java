package stormTP.operator;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MyTortoiseBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private int targetId;
    private String name;

    public MyTortoiseBolt(int id, String name) {
        this.targetId = id;
        this.name = name;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            // Lecture des champs enrichis par GiveRankBolt
            long id = input.getLongByField("id");

            if (id == targetId) {
                long top = input.getLongByField("top");
                String rang = input.getStringByField("rang");
                long total = input.getLongByField("total");
                // On passe le relais au bolt suivant
                collector.emit(input, new Values(id, top, rang, total, this.name));
            }
            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "rang", "total", "nom"));
    }
}