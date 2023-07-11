package mp.hadoop.project.mapper.Task5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LPGeneratorMapper extends
        Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tuple = value.toString().split("\t");
        String name= tuple[0];
        String label = tuple[1];
        context.write(new Text(label+":"),new Text(name));

    }
}