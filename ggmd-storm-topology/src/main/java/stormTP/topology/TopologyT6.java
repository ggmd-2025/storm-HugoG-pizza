package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import java.util.concurrent.TimeUnit;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.RankEvolutionBolt;
import stormTP.operator.Exit6Bolt;

public class TopologyT6 {
    
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("Usage: TopologyT6 <InputPort> <OutputPort>");
            System.exit(0);
        }
        
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        // 1. Spout
        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        // 2. Calcul des rangs
        builder.setBolt("ranker", new GiveRankBolt(), 1)
               .shuffleGrouping("masterStream");

        // 3. Filtre de notre tortue + transmission rang
        builder.setBolt("filter", new MyTortoiseBolt(3, "Donatello"), 1)
               .shuffleGrouping("ranker");

        // 4. Calcul de l'évolution (Windowed + Stateful)
        // Fenêtre de 10 secondes, recalculée toutes les 2 secondes
        builder.setBolt("evolution", new RankEvolutionBolt()
                        .withWindow(Duration.seconds(10), Duration.seconds(2))
                        .withMessageIdField("id"), 1) // messageId utile pour le stateful
               .shuffleGrouping("filter");

        // 5. Sortie
        builder.setBolt("exit", new Exit6Bolt(portOUTPUT), 1)
               .shuffleGrouping("evolution");

        Config config = new Config();
        config.setDebug(true);
        
        StormSubmitter.submitTopology("topoT6", config, builder.createTopology());
    }
}