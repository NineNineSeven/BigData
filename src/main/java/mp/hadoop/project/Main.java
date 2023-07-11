package mp.hadoop.project;

import org.apache.hadoop.util.ProgramDriver;

import mp.hadoop.project.job.PageRank;
import mp.hadoop.project.job.Task1;
import mp.hadoop.project.job.Task2;
import mp.hadoop.project.job.Task3;
import mp.hadoop.project.job.task5.LabelPropagation;

public class Main {

    public static void main(String[] args) throws Exception {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            // programDriver.addClass("wordcount", WordCount.class,
            // "count word of input files");
            programDriver.addClass("Task1", Task1.class, null);
            programDriver.addClass("Task2", Task2.class, null);
            programDriver.addClass("Task3", Task3.class, null);
            programDriver.addClass("PR", PageRank.class, "pagerank");
            programDriver.addClass("LP", LabelPropagation.class, "labelpropagation");
            exitCode = programDriver.run(args);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }

}
