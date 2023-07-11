package mp.hadoop.project.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //用制表符进行一次分割
            String[] words = value.toString().split("\t");
            //words[0]为格式为<A,B>的人名对，words[1]为出现次数（整数）
            //分别获取两个人名
            String[] names = words[0].substring(1, words[0].length() - 2).split(",");
            //将第二个人名与频数组合，中间用:连接
            String new_value = names[1] + ":" + words[1];
            //将第一个人名作为key，第二个人名与频数作为value
            context.write(new Text(names[0]), new Text(new_value));
        }
    }
