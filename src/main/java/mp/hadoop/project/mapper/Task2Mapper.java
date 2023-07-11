package mp.hadoop.project.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(",");
            //words中每一对人名进行一次输出。
            
            if(words.length < 2) return;
            for (int i = 0; i < words.length; i++) {
                for (int j = i + 1; j < words.length; j++) {
                    if(!words[i].equals(words[j])){
                        context.write(new Text(words[i] + "," + words[j]), new IntWritable(1));
                        context.write(new Text(words[j] + "," + words[i]), new IntWritable(1));
                    }
                }
            }
        }
}
