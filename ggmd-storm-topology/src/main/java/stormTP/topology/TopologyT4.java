package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.ComputeBonusBolt;
import stormTP.operator.Exit4Bolt;

public class TopologyT4 {
    
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("Usage: TopologyT4 <InputPort> <OutputPort>");
            System.exit(0);
        }
        
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        // 1. D'abord on calcule le rang pour TOUTES les tortues
        builder.setBolt("ranker", new GiveRankBolt(), 1)
               .shuffleGrouping("masterStream");

        // 2. Ensuite on filtre pour ne garder que la nôtre (ex: ID 3 "Donatello")
        // Attention : MyTortoiseBolt a été modifié pour accepter les tuples du ranker
        builder.setBolt("filter", new MyTortoiseBolt(3, "Donatello"), 1)
               .shuffleGrouping("ranker");

        // 3. On calcule le bonus (Bolt Stateful)
        builder.setBolt("bonus", new ComputeBonusBolt(), 1)
               .shuffleGrouping("filter");

        // 4. Sortie
        builder.setBolt("exit", new Exit4Bolt(portOUTPUT), 1)
               .shuffleGrouping("bonus");

        Config config = new Config();
        config.setDebug(true);
        
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
    }
}