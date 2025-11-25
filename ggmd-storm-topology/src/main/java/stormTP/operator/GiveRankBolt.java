package stormTP.operator;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class GiveRankBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    // Classe interne pour faciliter le tri
    private class RunnerData implements Comparable<RunnerData> {
        long id;
        long distance;
        long top;
        long total;
        long maxcel;

        public RunnerData(long id, long distance, long top, long total, long maxcel) {
            this.id = id;
            this.distance = distance;
            this.top = top;
            this.total = total;
            this.maxcel = maxcel;
        }

        @Override
        public int compareTo(RunnerData other) {
            // Tri décroissant sur la distance (le plus grand d'abord)
            return Long.compare(other.distance, this.distance);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String jsonStr = input.getStringByField("json");

        try (JsonReader jsonReader = Json.createReader(new StringReader(jsonStr))) {
            JsonObject root = jsonReader.readObject();

            if (root.containsKey("runners")) {
                JsonArray runnersArray = root.getJsonArray("runners");
                List<RunnerData> snapshot = new ArrayList<>();

                // 1. Extraction et calcul des distances pour tous les coureurs
                for (int i = 0; i < runnersArray.size(); i++) {
                    JsonObject obj = runnersArray.getJsonObject(i);
                    long id = obj.getInt("id");
                    long top = obj.getInt("top"); // Attention au type int/long selon le flux
                    long tour = obj.getInt("tour");
                    long cellule = obj.getInt("cellule");
                    long total = obj.getInt("total");
                    long maxcel = obj.getInt("maxcel");

                    long distance = (tour * maxcel) + cellule;
                    snapshot.add(new RunnerData(id, distance, top, total, maxcel));
                }

                // 2. Tri par distance décroissante
                Collections.sort(snapshot);

                // 3. Attribution des rangs et émission
                for (int i = 0; i < snapshot.size(); i++) {
                    RunnerData current = snapshot.get(i);
                    
                    // Le rang de base est l'index + 1
                    int rank = i + 1;
                    String rankStr = String.valueOf(rank);
                    
                    // Gestion des Ex-aequo
                    // On regarde s'il y a quelqu'un avec la même distance avant ou après
                    boolean isExAequo = false;
                    if (i > 0 && snapshot.get(i-1).distance == current.distance) isExAequo = true;
                    if (i < snapshot.size() - 1 && snapshot.get(i+1).distance == current.distance) isExAequo = true;
                    
                    // Si ex-aequo, le rang affiché est celui du premier du groupe
                    if (isExAequo) {
                        // On remonte pour trouver le "vrai" rang (le premier de l'égalité)
                        int trueRank = rank;
                        for (int j = i - 1; j >= 0; j--) {
                            if (snapshot.get(j).distance == current.distance) {
                                trueRank = j + 1;
                            } else {
                                break;
                            }
                        }
                        rankStr = trueRank + "ex";
                    }

                    // 4. Émission du tuple enrichi avec le rang
                    collector.emit(input, new Values(
                        current.id,
                        current.top,
                        rankStr,
                        current.total,
                        current.maxcel
                    ));
                }
            }
            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "rang", "total", "maxcel"));
    }
}