package mp.hadoop.project.mapper.Task4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mp.hadoop.project.utils.PersonLink;

public class PRMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    public void map(Text person, Text link_list, Context context) throws IOException, InterruptedException {
        String[] split = link_list.toString().split("[\\[\\]#|]");
        Double rank = Double.parseDouble(split[0]);
        for (int i = 1; i < split.length; i++) {
            PersonLink person_link = new PersonLink(split[i]);
            context.write(new Text(person_link.getLink_target_name()), new Text(String.valueOf(person_link.getRelation_ratio() * rank)));
        }
        context.write(person, link_list);
    }
}
