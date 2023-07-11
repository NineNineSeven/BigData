package mp.hadoop.project.reducer.Task5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import mp.hadoop.project.utils.NameParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String,Float> labelRank = new HashMap<String,Float>();
        Map<String,String> labelPerson = new HashMap<String,String>();
        ArrayList<NameParser> neighborList = new ArrayList<>();
        StringBuilder outNeighbors = new StringBuilder("");

        for (Text val : values) {
            String row = val.toString();
            if(row.charAt(0) == '1'){
                String[] fields = row.substring(1).split(",");
                String figure = fields[0];
                float chance = Float.parseFloat(fields[1]);
                NameParser neighbor = new NameParser(figure,chance);
                neighborList.add(neighbor);
                outNeighbors.append(neighbor.toString()).append("|");
            }
            else{
                String[] fields = row.substring(1).split(":");
                String name = fields[0];
                String label = fields[1];
                labelPerson.put(name,label);
            }
        }

        for(Map.Entry<String,String> entry : labelPerson.entrySet()){
            String person = entry.getKey();
            String label = entry.getValue();
            float weight = 0;
            for(NameParser val : neighborList){
                if(val.getName().equals(person)){
                    weight = val.getChance();
                    break;
                }
            }
            float current = labelRank.getOrDefault(label,0.f);
            labelRank.put(label,current + weight);
        }

        float maxWeight = 0.f;
        String newLabel = key.toString();
        for(Map.Entry<String,Float> entry : labelRank.entrySet()){
            if(entry.getValue() > maxWeight){
                maxWeight = entry.getValue();
                newLabel = entry.getKey();
            }
        }
        context.write(key,new Text(newLabel+"\t"+outNeighbors));
    }

}