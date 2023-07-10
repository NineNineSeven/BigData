package mp.hadoop.project.reducer.Task4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mp.hadoop.project.utils.PersonLink;

public class PRReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text person, Iterable<Text> ranks_link_list, Context context) throws IOException, InterruptedException {
        Double new_rank = .0;
        List<PersonLink> list = new ArrayList<>();
        for (Text val : ranks_link_list) {
            String str = val.toString();
            if (str.contains("[")) {
                String[] split = str.split("[\\[\\]#|]");
                for (int i = 1; i < split.length; i++)
                    list.add(new PersonLink(split[i]));
            } else
                new_rank += Double.parseDouble(str);
        }
        String list_str = "";
        for (PersonLink p : list)
            list_str += p.toString();
        list_str = list_str.substring(0, list_str.length() - 1);
        context.write(person, new Text(String.format("%.4f[%s]", new_rank, list_str)));
    }
}
