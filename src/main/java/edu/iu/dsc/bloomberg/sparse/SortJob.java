package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

    public class SortJob {
    private static final Logger LOG = Logger.getLogger(SortJob.class.getName());

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("input", true, "Input directory");
        options.addOption("output", true, "Output directory");
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = commandLineParser.parse(options, args);
        } catch (ParseException e) {
            LOG.log(Level.SEVERE, "Failed to read the options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("program", options);
            throw new RuntimeException(e);
        }


        JobConfig jobConfig = new JobConfig();
        jobConfig.put("input", cmd.getOptionValue("input"));
        jobConfig.put("output", cmd.getOptionValue("output"));


        Config config = ResourceAllocator.loadConfig(new HashMap<>());
        Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
        jobBuilder.setJobName("Bloombreg-sort");
        jobBuilder.setWorkerClass(SortData.class.getName());
        jobBuilder.addComputeResource(3, 15096, 4.0, 192);
        jobBuilder.setConfig(jobConfig);

        // now submit the job
        Twister2Submitter.submitJob(jobBuilder.build(), config);
    }
}
