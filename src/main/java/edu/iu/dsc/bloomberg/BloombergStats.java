package edu.iu.dsc.bloomberg;

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

public class BloombergStats extends TaskWorker {
    private static final Logger LOG = Logger.getLogger(BloombergStats.class.getName());

    @Override
    public void execute() {
        int parallism = 192;
        String filePath = config.getStringValue("input");
        String outFilePath = config.getStringValue("output");

        BaseSource readSource = new DataReadSourceTask("edge",filePath);
        BaseSink resultSink = new SinkTask();
        TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
        taskGraphBuilder.setMode(OperationMode.BATCH);
        taskGraphBuilder.addSource("source", readSource, parallism);
        ComputeConnection computeConnection = taskGraphBuilder.addSink("sink", resultSink, 1);
        computeConnection.reduce("source", "edge", Op.SUM, DataType.DOUBLE);

        DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
        ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
        taskExecutor.execute(dataFlowTaskGraph, executionPlan);
    }

    protected static class SinkTask extends BaseSink {
        private static final long serialVersionUID = -254264903510284798L;
        private static final Logger LOG = Logger.getLogger(SinkTask.class.getName());

        public boolean execute(IMessage message) {
            double[] data = (double[])message.getContent();
            double mean = data[0]/data[1];
            LOG.info("Mean : " + mean);
            return true;
        }
    }
}
