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

public class Exit2Bolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private int port;
    
    // Variables pour la gestion du réseau
    private ServerSocket serverSocket;
    private BufferedWriter out;

    public Exit2Bolt(int port) {
        this.port = port;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        try {
            // OUVERTURE DU PORT AU DEMARRAGE
            // On crée le serveur immédiatement pour que le ./startListner.sh puisse se connecter
            System.out.println("Exit2Bolt: Opening server socket on port " + port);
            this.serverSocket = new ServerSocket(port);
            
            // On lance un thread séparé pour accepter la connexion du client (le script listener)
            // Cela évite de bloquer le démarrage de Storm si le listener n'est pas encore lancé
            new Thread(() -> {
                try {
                    System.out.println("Exit2Bolt: Waiting for client listener...");
                    Socket clientSocket = serverSocket.accept(); // Bloque jusqu'à connexion
                    System.out.println("Exit2Bolt: Client connected!");
                    
                    this.out = new BufferedWriter(
                            new OutputStreamWriter(clientSocket.getOutputStream()));
                    
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error opening port " + port, e);
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            // 1. Récupération des champs envoyés par MyTortoiseBolt
            // Assure-toi que ces noms correspondent exactement à ceux déclarés dans MyTortoiseBolt
            long id = input.getLongByField("id");
            long top = input.getLongByField("top");
            String nom = input.getStringByField("nom");
            long nbCells = input.getLongByField("nbCellsParcourus");
            long total = input.getLongByField("total");
            long maxcel = input.getLongByField("maxcel");

            // 2. Construction de l'objet JSON attendu en sortie
            JsonObjectBuilder builder = Json.createObjectBuilder();
            builder.add("id", id);
            builder.add("top", top);
            builder.add("nom", nom);
            builder.add("nbCellsParcourus", nbCells);
            builder.add("total", total);
            builder.add("maxcel", maxcel);
            
            String jsonString = builder.build().toString();

            // 3. Envoi vers le script listener (si connecté)
            if (out != null) {
                try {
                    out.write(jsonString);
                    out.newLine(); // Important pour que readLine() fonctionne côté client
                    out.flush();
                } catch (IOException e) {
                    System.err.println("Erreur d'écriture vers le listener (client déconnecté ?)");
                    e.printStackTrace();
                    // On pourrait remettre out à null ici pour attendre une reconnexion
                }
            }

            // 4. Emission pour d'éventuels bolts suivants et acquittement
            collector.emit(input, new Values(jsonString));
            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
    
    @Override
    public void cleanup() {
        // Fermeture propre des ressources lors de l'arrêt de la topologie
        try {
            if (out != null) out.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}