package mp.hadoop.project.job.task5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mp.hadoop.project.mapper.Task5.LPGeneratorMapper;
import mp.hadoop.project.reducer.Task5.LPGeneratorReducer;

public class LPGenerator {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job3 = Job.getInstance(conf, "Generator");
        job3.setJarByClass(LPGenerator.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setMapperClass(LPGeneratorMapper.class);
        job3.setReducerClass(LPGeneratorReducer.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        job3.waitForCompletion(true);
    }
}
