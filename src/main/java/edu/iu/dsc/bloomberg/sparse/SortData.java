package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.bloomberg.BloombergStats;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

import java.util.logging.Logger;

public class SortData extends TaskWorker {
    private static final Logger LOG = Logger.getLogger(SortData.class.getName());

    @Override
    public void execute() {
        long startTime = System.currentTimeMillis();
        int parallism = 192;
        String filePath = config.getStringValue("input");
        String outFilePath = config.getStringValue("output");
        BaseSource readSource = new SortDataSource("edge");
        BaseSink resultSink = new SortData.SinkTask();

        TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
        taskGraphBuilder.setMode(OperationMode.BATCH);
        taskGraphBuilder.addSource("source", readSource, parallism);
        ComputeConnection computeConnection = taskGraphBuilder.addSink("sink", resultSink, parallism);
        computeConnection.reduce("source", "edge", Op.SUM, DataType.DOUBLE);
        DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
        ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
        taskExecutor.execute(dataFlowTaskGraph, executionPlan);
        LOG.info("Total time taken : " + (System.currentTimeMillis() - startTime));

    }

    protected static class SinkTask extends BaseSink {
        private static final long serialVersionUID = -254264903510284798L;
        private static final Logger LOG = Logger.getLogger(SortData.SinkTask.class.getName());

        public boolean execute(IMessage message) {
            double[] data = (double[]) message.getContent();
            LOG.info("Total Count Doubles: " + data[0]);
            return true;
        }
    }
}
