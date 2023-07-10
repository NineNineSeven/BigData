package mp.hadoop.project.reducer.Task4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RVReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    public void reduce(DoubleWritable rank, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
        double r = rank.get();
        for (Text person : persons) {
            context.write(person, new DoubleWritable(-r));
        }
    }
}
