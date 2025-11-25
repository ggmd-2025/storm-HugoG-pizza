package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.Exit3Bolt;

public class TopologyT3 {
    
    public static void main(String[] args) throws Exception {
        // Récupération des arguments
        if(args.length < 2){
            System.out.println("Usage: TopologyT3 <InputPort> <OutputPort>");
            System.exit(0);
        }
        
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        // 1. Spout (Ecoute le flux JSON)
        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        // 2. Calcul du rang (Reçoit le gros JSON, émet des tuples classés)
        // On garde un parallélisme de 1 ici par simplicité, 
        // mais comme on traite le tableau complet, ça marcherait aussi en >1 (shuffleGrouping).
        builder.setBolt("ranker", new GiveRankBolt(), 1)
               .shuffleGrouping("masterStream");

        // 3. Sortie (Formatage JSON final et envoi Socket)
        builder.setBolt("exit", new Exit3Bolt(portOUTPUT), 1)
               .shuffleGrouping("ranker");

        Config config = new Config();
        config.setDebug(true);
        
        // Soumission
        StormSubmitter.submitTopology("topoT3", config, builder.createTopology());
    }
}