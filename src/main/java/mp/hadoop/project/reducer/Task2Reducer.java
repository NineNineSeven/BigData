package mp.hadoop.project.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //统计每个人名对出现的次数
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write( new Text("<" + key.toString() + "> "), new IntWritable(count));

        }
    }
