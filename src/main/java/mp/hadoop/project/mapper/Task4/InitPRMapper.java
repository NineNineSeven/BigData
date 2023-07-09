package mp.hadoop.project.mapper.Task4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitPRMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    public void map(Text person, Text link_list, Context context) throws IOException, InterruptedException {
        context.write(person, link_list);
    }
}
