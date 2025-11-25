package stormTP.core;

import java.io.StringReader;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class Manager {
    
    public static final String CONST = "C'est mieux!";
    public static final String PROG = "Statu quo";
    public static final String REGR = "ça sera sûrement mieux plus tard!";

    long dossard = -1;

    public Manager(){
    }

    public Manager(long dossard){
        this.dossard = dossard;
    }
    
    /**
     * Parse le JSON et renvoie un objet Runner si l'ID correspond, sinon null.
     */
    public Runner filter(int targetId, String input){
        Runner animal = null;
        
        try (JsonReader jsonReader = Json.createReader(new StringReader(input))) {
            JsonObject obj = jsonReader.readObject();
            
            long id = obj.getInt("id");
            
            // On ne crée l'objet que si c'est la tortue recherchée
            if (id == targetId) {
                String nom = "Tortue-" + id; // Nom par défaut ou à récupérer si présent
                // Note : le champ "nom" n'est pas dans le flux d'entrée standard décrit, 
                // on le génère ou on le passe en paramètre ailleurs.
                
                long top = obj.getInt("top");
                int tour = obj.getInt("tour");
                int cellule = obj.getInt("cellule");
                int total = obj.getInt("total");
                // maxcel n'est pas dans le constructeur de Runner fourni, on l'ajoute manuellement si besoin
                // ou on modifie Runner. Pour l'instant, on fait avec ce qu'on a.
                
                animal = new Runner(id, nom, "tortoise", top, cellule, tour, total);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return animal;
    }
}