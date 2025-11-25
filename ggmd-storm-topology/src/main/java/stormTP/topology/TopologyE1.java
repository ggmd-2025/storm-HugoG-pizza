package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.ConsumeTimeBolt;
import stormTP.operator.ExitInLogBolt;

public class TopologyE1 {
    
    public static void main(String[] args) throws Exception {

        // --- MODIFICATION ICI : ON PASSE A 4 EXECUTORS ---
        int nbExecutors = 4; 
        
        int portINPUT = Integer.parseInt(args[0]);

        /* Création du spout */
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        /* Création de la topologie */
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("masterStream", spout);
        
        /* Opérateur qui consomme du temps */
        // Le paramètre nbExecutors (4) indique à Storm de créer 4 threads pour ce Bolt
        builder.setBolt("consume", new ConsumeTimeBolt(), nbExecutors)
               .shuffleGrouping("masterStream");
        
        /* OutPut mis dans le log */
        builder.setBolt("exit", new ExitInLogBolt(), nbExecutors)
               .shuffleGrouping("consume");
           
        Config config = new Config();
        
        // --- MODIFICATION ICI : ON PASSE A 4 WORKERS ---
        config.setNumWorkers(4);
        
        // On remonte un peu le timeout pour voir si le parallélisme permet de sauver des tuples
        // On le met à 10s (supérieur aux ~7s de latence observée)
        config.setMessageTimeoutSecs(10); 
        
        StormSubmitter.submitTopology("topoE1", config, builder.createTopology());
    }
}