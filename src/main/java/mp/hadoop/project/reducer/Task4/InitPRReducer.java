package mp.hadoop.project.reducer.Task4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitPRReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text person, Iterable<Text> link_list, Context context) throws IOException, InterruptedException {
        // link_list按理来说应该是只有一个
        for (Text val : link_list)
            context.write(person, new Text("1" + val.toString()));
    }
}
