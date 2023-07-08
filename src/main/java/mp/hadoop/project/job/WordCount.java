package mp.hadoop.project.job;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mp.hadoop.project.mapper.TokenizerMapper;
import mp.hadoop.project.reducer.IntSumReducer;
import mp.hadoop.project.utils.ArgParser;

public class WordCount {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);

    job.setMapperClass(TokenizerMapper.class);

    job.setCombinerClass(IntSumReducer.class);

    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    CommandLine cmd = ArgParser.parse(otherArgs);
    FileInputFormat.addInputPath(job, new Path(cmd.getOptionValue("input")));
    FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("output")));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
