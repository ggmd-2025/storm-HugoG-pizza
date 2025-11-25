package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.SpeedBolt;
import stormTP.operator.Exit5Bolt;

public class TopologyT5 {
    
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("Usage: TopologyT5 <InputPort> <OutputPort>");
            System.exit(0);
        }
        
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", portINPUT));

        // 1. On filtre UNE tortue (sinon le windowing mélangerait les ID)
        builder.setBolt("filter", new MyTortoiseBolt(3, "Donatello"), 1)
               .shuffleGrouping("masterStream");

        // 2. Bolt Fenêtré
        // withWindow(Length, SlidingInterval)
        // Length = Count.of(10) -> Sur les 10 derniers tuples reçus
        // Sliding = Count.of(5) -> Calculé tous les 5 tuples reçus
        builder.setBolt("speed", new SpeedBolt().withWindow(Count.of(10), Count.of(5)), 1)
               .shuffleGrouping("filter");

        // 3. Sortie
        builder.setBolt("exit", new Exit5Bolt(portOUTPUT), 1)
               .shuffleGrouping("speed");

        Config config = new Config();
        config.setDebug(true);
        
        StormSubmitter.submitTopology("topoT5", config, builder.createTopology());
    }
}