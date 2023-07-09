package mp.hadoop.project.mapper.Task4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RVMapper extends Mapper<Text, Text, DoubleWritable, Text> {
    @Override
    public void map(Text person, Text link_list, Context context) throws IOException, InterruptedException {
        String[] split = link_list.toString().split("[\\[\\]#|]");
        System.out.println(person);
        System.out.println(split[0]);
        context.write(new DoubleWritable(-Double.parseDouble(split[0])), person);
    }
}
