package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.typed.batch.BPartitionKeyedCompute;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;

public class SparseGenSinkTask extends BPartitionKeyedCompute<Integer, int[]>
        implements ISink {
    private static final Logger LOG = Logger.getLogger(SparseGenSinkTask.class.getName());
    private final int round;
    String filePath = "/scratch_hdd/bloomberg/";

    public SparseGenSinkTask(int round) {
        this.round = round;
    }

    @Override
    public boolean keyedPartition(Iterator<Tuple<Integer, int[]>> content) {
        try {
            PrintWriter outWriter = new PrintWriter(new FileWriter(filePath + "part_" + context.taskId() + "__" + round));

            long count = 0;
            Tuple<Integer, int[]> tuple;
            while (content.hasNext()) {
                tuple = content.next();
                outWriter.println(tuple.getKey() + " " + tuple.getValue()[0]
                        + " " + tuple.getValue()[1]);
                count++;
            }
            outWriter.flush();
            outWriter.close();
            LOG.info(String.format("%d received keyed-partition count : %d",
                    context.taskId(), count));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }
}
