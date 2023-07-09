package mp.hadoop.project;

import org.apache.hadoop.util.ProgramDriver;

import mp.hadoop.project.job.PageRank;

public class Main {

    public static void main(String[] args) throws Exception {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            // programDriver.addClass("wordcount", WordCount.class,
            // "count word of input files");
            programDriver.addClass("PR", PageRank.class, "pagerank");
            exitCode = programDriver.run(args);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }

}
