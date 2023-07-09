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
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1 {


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.addCacheFile(new URI("hdfs://master001:9000/data/2023s/exp2/person_name_list.txt"));
        job.setJarByClass(Task1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Task1Mapper.class);
        job.setReducerClass(Task1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class Task1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        //全名列表
        Vector<String> fullNames = new Vector<>();
        //名列表
        //Vector<String> singleNames = new Vector<>();
        Set<String> singleNames = new HashSet<>();
        Map<String,String> Sec2First = new HashMap<>();

        @Override                                                               
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if(cacheFiles == null || cacheFiles.length <= 0){
                System.out.println("cacheFiles is null");
                return;
            }

            FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
            FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = bufferedReader.readLine()) != null){
                String[] names = line.split(" ");
                if(names[0].equals("Professor") && names.length > 1){
                    //将Professor删掉，后面的名字前移
                    for(int i = 0; i < names.length - 1; i++){
                        names[i] = names[i + 1];
                    }
                    //将最后一个名字置空
                    names[names.length - 1] = "";
                }
                singleNames.add(names[0]);
                if(names.length > 1){
                    fullNames.add(line);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            //获取这个文件的文件名，并于这个文段的编号连接，作为key
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String keyStr = fileName + "#" + key.toString();

            text.replace("Lily Potter", "Lily");
            text.replace("James Potter", "James");
            text.replace("Harry Potter", "Harry");
            text.replace("Potter", "Harry");

            for(String LongName : fullNames){
                String[] names = LongName.split(" ");
                text.replace(LongName, names[0]); 
                text.replace(names[1], names[0]);
            }

            StringTokenizer st = new StringTokenizer(text);
            
            while(st.hasMoreTokens()){
                String word = st.nextToken();
                if(singleNames.contains(word)){
                    if(word.equals("Potter"))
                        word = "Harry";
                    context.write(new Text(keyStr), new Text(word));
                }
            }
        }
    }

    public static class Task1Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ans = "";
            for (Text value : values) {
                if (ans.length() > 0)
                    ans += ",";
                ans += value.toString();
            }
            context.write(NullWritable.get(), new Text(ans));
        }
    }
}
