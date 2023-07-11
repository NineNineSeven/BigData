package mp.hadoop.project.reducer;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //输出格式:Harry\t[Ron,0.66667|Hermione,0.33333]
            //统计每个人名对出现的次数
            int count = 0;
            String ans = "";
            Vector<String> name_val = new Vector<String>();
            for (Text value : values) {
                String[] name = value.toString().split(":");
                count += Integer.parseInt(name[1]);
                name_val.add(value.toString());
            }
            //计算每个人的概率
            for (String name : name_val) {
                if (ans.equals("") == false){
                    ans += "|";
                }
                String[] name_count = name.split(":");
                double prob = Double.parseDouble(name_count[1]) / count;
                ans += name_count[0] + "," + String.format("%.5f", prob);
            }

            context.write(key, new Text("[" + ans + "]"));
        }
    }
