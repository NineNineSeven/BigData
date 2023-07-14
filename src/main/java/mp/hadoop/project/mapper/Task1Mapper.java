package mp.hadoop.project.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class Task1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        //全名列表
        Vector<String> fullNames = new Vector<>();
        //名列表
        //Vector<String> singleNames = new Vector<>();
        Set<String> singleNames = new HashSet<>();
        Vector<String> nameFrom = new Vector<>();
        Vector<String> nameto = new Vector<>();

        @Override                                                               
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            nameFrom.add("Lily Potter");nameto.add("Lily");
            nameFrom.add("James Potter");nameto.add("James");
            nameFrom.add("Harry Potter");nameto.add("Harry");
            nameFrom.add("Potter");nameto.add("Harry");


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
                if(names[0].equals("Professor") && names.length > 1)
                {
                    //删掉line中的Professor
                    line = line.substring(10);
                }
                if(names[0].equals("Mr") && names.length > 1)
                {
                    //删掉line中的Mr
                    line = line.substring(3);
                }
                if(names[0].equals("Madam") && names.length > 1)
                {
                    //删掉line中的Madam
                    line = line.substring(6);
                }
                if(names[0].equals("Sir") && names.length > 1)
                {
                    //删掉line中的Sir
                    line = line.substring(4);
                }
                names = line.split(" ");
                singleNames.add(names[0]);
                if(names.length > 1){
                    nameFrom.add(line);
                    nameto.add(names[0]);
                    nameFrom.add(names[1]);
                    nameto.add(names[0]);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            //获取这个文件的文件名，并于这个文段的编号连接，作为key
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            if(fileName.contains("person_name_list"))return;
            String keyStr = fileName + "#" + key.toString();

            //将text中所有的符号替换为空格，除了-
            text = text.replaceAll("[^a-zA-Z0-9-]", " ");
            //将text中连续的空格替换为一个空格
            text = text.replaceAll("\\s+", " ");

            for(int i = 0; i < nameFrom.size(); i++){
                text = text.replaceAll(nameFrom.get(i), nameto.get(i));
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