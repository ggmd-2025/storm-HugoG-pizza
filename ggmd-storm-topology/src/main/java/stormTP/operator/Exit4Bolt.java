package stormTP.operator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObjectBuilder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Exit4Bolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private int port;
    private ServerSocket serverSocket;
    private BufferedWriter out;

    public Exit4Bolt(int port) {
        this.port = port;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            System.out.println("Exit4Bolt: Opening server socket on port " + port);
            this.serverSocket = new ServerSocket(port);
            new Thread(() -> {
                try {
                    System.out.println("Exit4Bolt: Waiting for client...");
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Exit4Bolt: Client connected!");
                    this.out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        } catch (IOException e) {
            throw new RuntimeException("Error opening port " + port, e);
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            long id = input.getLongByField("id");
            String tops = input.getStringByField("tops");
            String nom = input.getStringByField("nom");
            long score = input.getLongByField("score");

            JsonObjectBuilder builder = Json.createObjectBuilder();
            builder.add("id", id);
            builder.add("nom", nom);
            builder.add("tops", tops);
            builder.add("score", score); // Le score cumulé issu de l'état
            
            String jsonString = builder.build().toString();

            if (out != null) {
                out.write(jsonString);
                out.newLine();
                out.flush();
            }

            collector.emit(input, new Values(jsonString));
            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
    
    @Override
    public void cleanup() {
        try {
            if (out != null) out.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {}
    }
}