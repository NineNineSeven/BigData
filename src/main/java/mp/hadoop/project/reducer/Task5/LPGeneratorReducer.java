package mp.hadoop.project.reducer.Task5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LPGeneratorReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder peopleList = new StringBuilder();
        for(Text val : values){
            peopleList.append(val.toString()).append("\n");
        }
        context.write(key, new Text(peopleList.toString()));
    }
}