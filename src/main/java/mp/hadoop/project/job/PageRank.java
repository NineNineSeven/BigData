package mp.hadoop.project.job;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mp.hadoop.project.mapper.Task4.InitPRMapper;
import mp.hadoop.project.mapper.Task4.PRMapper;
import mp.hadoop.project.mapper.Task4.RVMapper;
import mp.hadoop.project.reducer.Task4.InitPRReducer;
import mp.hadoop.project.reducer.Task4.PRReducer;
import mp.hadoop.project.reducer.Task4.RVReducer;
import mp.hadoop.project.utils.PRParser;

public class PageRank {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        PRParser prp = new PRParser(remainingArgs);
        String input = prp.getInput();
        String output = prp.getOutput();
        Integer r_cnt = prp.getRepeat();

        // Init PageRank input job
        Job init_pr_job = Job.getInstance(conf, "init PageRank input");
        init_pr_job.setJarByClass(PageRank.class);

        init_pr_job.setMapperClass(InitPRMapper.class);
        init_pr_job.setMapOutputKeyClass(Text.class);
        init_pr_job.setMapOutputValueClass(Text.class);

        init_pr_job.setReducerClass(InitPRReducer.class);
        init_pr_job.setOutputKeyClass(Text.class);
        init_pr_job.setOutputKeyClass(Text.class);
        
        init_pr_job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(init_pr_job, new Path(input));
        FileOutputFormat.setOutputPath(init_pr_job, new Path(output + "/mid_res/m0"));

        ControlledJob c_init_pr_job = new ControlledJob(conf);
        c_init_pr_job.setJob(init_pr_job);

        // PageRanklter job
        List<ControlledJob> c_pr_job_list = new ArrayList<>();
        c_pr_job_list.add(c_init_pr_job);
        for (int i = 0; i < r_cnt; i++) {
            Job pr_job = Job.getInstance(conf, "PageRanklter: " + (i + 1));
            pr_job.setJarByClass(PageRank.class);

            pr_job.setMapperClass(PRMapper.class);
            pr_job.setMapOutputKeyClass(Text.class);
            pr_job.setMapOutputValueClass(Text.class);

            pr_job.setReducerClass(PRReducer.class);
            pr_job.setOutputKeyClass(Text.class);
            pr_job.setOutputKeyClass(Text.class);

            pr_job.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(pr_job, new Path(output + "/mid_res/m" + i));
            FileOutputFormat.setOutputPath(pr_job, new Path(output + "/mid_res/m" + (i + 1)));

            ControlledJob c_pr_job = new ControlledJob(conf);
            c_pr_job.setJob(pr_job);

            c_pr_job.addDependingJob(c_pr_job_list.get(i));

            c_pr_job_list.add(c_pr_job);
        }

        // RankView job
        Job rv_job = Job.getInstance(conf, "Rank View");
        rv_job.setJarByClass(PageRank.class);

        rv_job.setMapperClass(RVMapper.class);
        rv_job.setMapOutputKeyClass(DoubleWritable.class);
        rv_job.setMapOutputValueClass(Text.class);

        rv_job.setReducerClass(RVReducer.class);
        rv_job.setOutputKeyClass(Text.class);
        rv_job.setOutputValueClass(DoubleWritable.class);

        rv_job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(rv_job, new Path(output + "/mid_res/m" + r_cnt));
        FileOutputFormat.setOutputPath(rv_job, new Path(output + "/result"));

        ControlledJob c_rv_job = new ControlledJob(conf);
        c_rv_job.setJob(rv_job);

        c_rv_job.addDependingJob(c_pr_job_list.get(r_cnt));

        // Jobs controller
        JobControl jc = new JobControl("PageRank");
        jc.addJob(c_init_pr_job);
        c_pr_job_list.forEach(jc::addJob);
        jc.addJob(c_rv_job);

        Thread thread = new Thread(jc);
        thread.start();

        while (true) {
            if (jc.allFinished()) {
                System.out.println("全部执行完毕");
                jc.stop();
                break;
            }
        }
    }
}
