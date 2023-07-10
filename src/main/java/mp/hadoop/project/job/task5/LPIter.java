package mp.hadoop.project.task5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import mp.hadoop.project.utils.NameParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mp.hadoop.project.mapper.Task5.IterMapper;
import mp.hadoop.project.reducer.Task5.IterReducer;

public class LPIter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job2 = new Job(conf, "Iteration");
        job2.setJarByClass(LPIter.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(IterMapper.class);
        job2.setReducerClass(IterReducer.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
    }
}