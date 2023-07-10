package mp.hadoop.project.task5;

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

public class LabelPropagation {

    public static void main(String[] args) throws Exception {
        String[] InitFP = { args[0], args[1] + "/Iter0" };
        LPInitialization.main(InitFP);
        int times = Integer.parseInt(args[2]);
        String[] IterFilePath = { "", "" };
        for (int i = 0; i < times; i++) {
            IterFilePath[0] = args[1] + "/Iter" + i;
            IterFilePath[1] = args[1] + "/Iter" + String.valueOf(i + 1);
            LPIter.main(IterFilePath);
        }
        //
        String[] ResFilePath = { args[1] + "/Iter" + times, args[1] + "/Result" };
        LPGenerator.main(ResFilePath);
    }
}