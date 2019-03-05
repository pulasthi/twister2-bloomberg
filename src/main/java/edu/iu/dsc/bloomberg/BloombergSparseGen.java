package edu.iu.dsc.bloomberg;

import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.task.api.BaseSource;

import java.util.logging.Logger;

public class BloombergSparseGen extends TaskWorker {
    private static final Logger LOG = Logger.getLogger(BloombergSparseGen.class.getName());

    @Override
    public void execute() {
        int parallism = 192;
        String filePath = config.getStringValue("input");
        String outFilePath = config.getStringValue("output");
        double min = 0.61600;
        double max = 14950.00;
        BaseSource readSource = new SparseGenSourceTask("edge",filePath, min, max);

    }
}
