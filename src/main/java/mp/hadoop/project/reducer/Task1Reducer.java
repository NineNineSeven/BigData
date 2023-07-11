package mp.hadoop.project.reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Reducer;


public class Task1Reducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ans = "";
            for (Text value : values) {
                if (ans.length() > 0)
                    ans += ",";
                ans += value.toString();
            }
            context.write(NullWritable.get(), new Text(ans));
        }
}
