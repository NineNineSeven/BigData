package mp.hadoop.project.job.task5;

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