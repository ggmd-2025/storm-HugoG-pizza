package stormTP.operator;

import java.util.List;
import java.util.Map;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class RankEvolutionBolt extends BaseStatefulWindowedBolt<KeyValueState<String, String>> {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private KeyValueState<String, String> state;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, String> state) {
        this.state = state;
        // On pourrait stocker des stats historiques ici si besoin
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        
        // Il nous faut au moins une évolution (2 tuples) pour comparer
        if (tuples.size() >= 2) {
            Tuple startTuple = tuples.get(0);
            Tuple endTuple = tuples.get(tuples.size() - 1);

            String rangStartStr = startTuple.getStringByField("rang");
            String rangEndStr = endTuple.getStringByField("rang");

            // Nettoyage des "ex"
            int rStart = parseRank(rangStartStr);
            int rEnd = parseRank(rangEndStr);

            String evolution = "Constant";
            if (rEnd < rStart) {
                // Le rang a diminué (ex: passer de 3ème à 1er), c'est une progression !
                evolution = "En progression";
            } else if (rEnd > rStart) {
                // Le rang a augmenté (ex: passer de 1er à 3ème), c'est une régression.
                evolution = "En régression";
            }

            long id = endTuple.getLongByField("id");
            String nom = endTuple.getStringByField("nom");
            
            // On utilise le timestamp système pour la "date"
            String date = String.valueOf(System.currentTimeMillis());

            collector.emit(new Values(id, nom, date, evolution));
        }
    }

    private int parseRank(String rankStr) {
        if (rankStr.endsWith("ex")) {
            return Integer.parseInt(rankStr.substring(0, rankStr.length() - 2));
        }
        return Integer.parseInt(rankStr);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "nom", "date", "evolution"));
    }
}