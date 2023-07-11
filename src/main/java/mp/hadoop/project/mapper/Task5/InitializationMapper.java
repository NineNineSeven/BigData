package mp.hadoop.project.mapper.Task5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitializationMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String name = fields[0];
        String label = name;
        String tmp = fields[1].substring(1);
        // String tmp = tuple[2].substring(1);
        tmp = tmp.substring(0, tmp.length() - 1);
        String val = label + "\t" + tmp;
        context.write(new Text(name),new Text(val));
    }
}