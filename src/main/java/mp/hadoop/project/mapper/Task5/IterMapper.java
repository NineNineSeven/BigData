package mp.hadoop.project.mapper.Task5;

import java.io.IOException;
import java.util.ArrayList;

import mp.hadoop.project.utils.NameParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends
        Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String row = value.toString();
        String[] tuple = row.split("\t");
        String name = tuple[0];
        String label = tuple[1];
        ArrayList<NameParser> neighborList = new ArrayList<>();
        // String tmp = tuple[2].substring(1);
        // tmp = tmp.substring(0, tmp.length() - 1);
        String[] neighbors = tuple[2].split("\\|");
        for(String neighbor : neighbors){
            String[] fields = neighbor.split(",");
            String figure = fields[0];
            float chance = Float.parseFloat(fields[1]);
            neighborList.add(new NameParser(figure,chance));
        }
        // mapper输出，前缀为1代表人物姓名加邻居名和权值，为2代表邻居名加人物姓名和人物标签
        for(NameParser neighbor : neighborList){
            context.write(new Text(name),new Text("1"+neighbor.toString()));
            context.write(new Text(neighbor.getName()),new Text("2"+name+":"+label));
        }

    }
}