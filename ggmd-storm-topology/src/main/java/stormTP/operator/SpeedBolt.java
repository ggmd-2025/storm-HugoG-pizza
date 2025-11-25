package stormTP.operator;

import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class SpeedBolt extends BaseWindowedBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        
        // On a besoin d'au moins 2 tuples pour calculer une vitesse
        if (tuples.size() >= 2) {
            // Le premier tuple de la fenêtre (le plus vieux)
            Tuple startTuple = tuples.get(0);
            // Le dernier tuple de la fenêtre (le plus récent)
            Tuple endTuple = tuples.get(tuples.size() - 1);

            long startDist = startTuple.getLongByField("nbCellsParcourus");
            long endDist = endTuple.getLongByField("nbCellsParcourus");
            
            long startTop = startTuple.getLongByField("top");
            long endTop = endTuple.getLongByField("top");

            long distDiff = endDist - startDist;
            long topDiff = endTop - startTop;

            double vitesse = 0.0;
            if (topDiff > 0) {
                vitesse = (double) distDiff / topDiff;
            }

            // Récupération des infos constantes
            long id = startTuple.getLongByField("id");
            String nom = startTuple.getStringByField("nom");
            String topsLabel = startTop + "-" + endTop;

            collector.emit(new Values(id, nom, topsLabel, vitesse));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "nom", "tops", "vitesse"));
    }
}