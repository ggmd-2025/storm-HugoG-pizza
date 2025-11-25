package stormTP.operator;

import java.util.Map;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ComputeBonusBolt extends BaseStatefulBolt<KeyValueState<String, Long>> {

    private static final long serialVersionUID = 1L;
    private KeyValueState<String, Long> kvState;
    private long score; // Variable locale pour performance, synchronisée avec le State
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, Long> state) {
        this.kvState = state;
        // On récupère le score sauvegardé, ou 0 si c'est le début
        this.score = kvState.get("score", 0L);
        System.out.println("DEBUG: Init state score = " + score);
    }

    @Override
    public void execute(Tuple input) {
        try {
            long id = input.getLongByField("id");
            long top = input.getLongByField("top");
            String rangStr = input.getStringByField("rang");
            long total = input.getLongByField("total");
            String nom = input.getStringByField("nom");

            // Nettoyage du rang (ex: "3ex" -> 3)
            if (rangStr.endsWith("ex")) {
                rangStr = rangStr.substring(0, rangStr.length() - 2);
            }
            int rank = Integer.parseInt(rangStr);

            // LOGIQUE MÉTIER :
            // Tous les 15 tops, on donne des points.
            if (top > 0 && top % 15 == 0) {
                
                // Formule : total - rang (ex: 10 - 1 = 9 pts pour le premier)
                long pointsGagnes = total - rank;
                if (pointsGagnes < 0) pointsGagnes = 0;

                score += pointsGagnes;
                
                // MISE À JOUR DE L'ÉTAT (Persistance)
                kvState.put("score", score);

                // Format "ti-ti+9" demandé, ici adapté à la période 15
                // Ex: pour top 15, on affiche "1-15" ou "15-24" selon l'interprétation.
                // Logiquement pour un bilan passé :
                String topsLabel = (top - 14) + "-" + top;

                collector.emit(input, new Values(id, topsLabel, nom, score));
            }

            // Note : BaseStatefulBolt gère souvent l'ack automatiquement, 
            // mais l'ajouter explicitement ne fait pas de mal dans ce contexte TP.
            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "tops", "nom", "score"));
    }
}