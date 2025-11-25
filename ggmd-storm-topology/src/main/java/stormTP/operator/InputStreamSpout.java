package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

public class InputStreamSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger("StreamSimSpoutLogger");
    private SpoutOutputCollector collector;

    private String host;
    private int port;
    private Socket socket;
    private BufferedReader reader;
    
    // Variable pour mesurer le temps avant congestion
    private long startTime;

    public InputStreamSpout(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.startTime = System.currentTimeMillis(); // On top le départ

        try {
            socket = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("TCP Spout connected to " + host + ":" + port);
        } catch (Exception e) {
            throw new RuntimeException("Cannot connect to socket", e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            if (reader.ready()) {
                String json = reader.readLine();
                if (json != null) {
                    // On utilise un ID de message pour pouvoir tracer les échecs (fail)
                    // Ici, on utilise le hashCode du json ou un compteur simple comme ID
                    Object msgId = json.hashCode(); 
                    collector.emit(new Values(json), msgId);
                }
            }
            // Petite pause indispensable pour ne pas saturer le CPU à vide
            Utils.sleep(5); 

        } catch (Exception e) {
            e.printStackTrace();
            Utils.sleep(1000);
        }
    }

    @Override
    public void ack(Object msgId) {
        // Le tuple a été traité avec succès
        // logger.info("ACK: " + msgId); 
    }

    @Override
    public void fail(Object msgId) {
        // C'est ICI que l'on détecte la congestion.
        // Si Storm appelle fail(), c'est que le tuple n'a pas été traité dans les temps (timeout).
        long currentTime = System.currentTimeMillis();
        long durationSeconds = (currentTime - startTime) / 1000;
        
        logger.info("[ConsumeTime] Failure (msg: " + msgId + " ) after " + durationSeconds + " seconds");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
    
    @Override
    public void close() {
        try {
            if (socket != null) socket.close();
        } catch(Exception e) {}
    }

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}
}