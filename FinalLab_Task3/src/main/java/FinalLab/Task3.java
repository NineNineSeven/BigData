package FinalLab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3 {


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Task3.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Task3Mapper.class);
        job.setReducerClass(Task3Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class Task3Mapper extends Mapper<Object, Text, Text, Text> {

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

    public static class Task3Reducer extends Reducer<Text, Text, Text, Text> {
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
}
