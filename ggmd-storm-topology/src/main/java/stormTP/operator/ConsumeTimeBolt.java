package stormTP.operator;

import java.io.StringReader;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ConsumeTimeBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    // private static final Logger logger = Logger.getLogger("ConsumeTimeBolt");

    @Override
    public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        try {
            String n = t.getValueByField("json").toString();

            JsonReader jr = Json.createReader(new StringReader(n));
            JsonObject obj = jr.readObject();

            int cpt = 0;
            String indice = null;

            JsonObjectBuilder r = Json.createObjectBuilder();

            for (String key : obj.keySet()) {
                String value = obj.get(key).toString();
                
                // --- SABOTAGE : On répète le cryptage pour charger le CPU ---
                // On crypte 5000 fois chaque valeur pour simuler un calcul lourd
                String encryptedVal = value;
                for(int i = 0; i < 1000000; i++) {
                    encryptedVal = encrypter(encryptedVal);
                }
                
                indice = "" + cpt++;
                r.add(indice, encryptedVal);
            }

            JsonObject row = r.build();
            collector.emit(t, new Values(row.toString()));
            collector.ack(t);
            
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(t);
        }
    }

    private String encrypter(String val) {
        String res = "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(val.getBytes());
            res = Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
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