package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

import java.util.Comparator;
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
        long startTime = System.currentTimeMillis();
        BaseSource readSource = new SparseGenSourceTask("edge", filePath, min, max);
        SparseGenSinkTask baseSink = new SparseGenSinkTask();
        TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
        taskGraphBuilder.addSource("source", readSource, parallism);
        ComputeConnection computeConnection = taskGraphBuilder.addSink("sink", baseSink, parallism);
        computeConnection.keyedGather("source", "edge", DataType.INTEGER, DataType.INTEGER, true,
                Comparator.comparingInt(o -> (Integer) o));
        taskGraphBuilder.setMode(OperationMode.BATCH);

        DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
        ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
        taskExecutor.execute(dataFlowTaskGraph, executionPlan);
        LOG.info("Total time taken : " + (System.currentTimeMillis() - startTime));
    }
}
