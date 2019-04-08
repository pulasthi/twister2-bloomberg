package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.typed.batch.BPartitionKeyedCompute;

import java.util.Iterator;
import java.util.logging.Logger;

public class SparseGenSinkTask extends BPartitionKeyedCompute<Integer, int[]>
        implements ISink {
    private static final Logger LOG = Logger.getLogger(SparseGenSinkTask.class.getName());

    @Override
    public boolean keyedPartition(Iterator<Tuple<Integer, int[]>> content) {
        LOG.info(String.format("%d received keyed-partition %d",
                context.getWorkerId(), context.taskId()));
        return true;
    }
}
