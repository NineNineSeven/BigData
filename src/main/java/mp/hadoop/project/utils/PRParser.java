package mp.hadoop.project.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class PRParser {
    private CommandLine cmd;

    public PRParser(String[] args) {
        Options options = new Options();

        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("o", "output", true, "output file");
        output.setRequired(true);
        options.addOption(output);

        Option repeat = new Option("r", "repeat-cnt", true, "the Ranklter repeat count");
        repeat.setRequired(true);
        options.addOption(repeat);

        CommandLineParser parser = new PosixParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }
    }

    public String getInput() {
        return cmd.getOptionValue("input");
    }

    public String getOutput() {
        return cmd.getOptionValue("output");
    }

    public Integer getRepeat() {
        return Integer.parseInt(cmd.getOptionValue("repeat-cnt"));
    }
}
