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

public class BloombergStats extends TaskWorker {

    public void execute() {
        int parallism = 192;
        String filePath = "/share/project2/FG546/pulasthi/bloomberg";
        String outFilePath = "/share/project2/FG546/pulasthi/bloomberg/toutput";

        BaseSource readSource = new DataReadSourceTask("edge",filePath);
        BaseSink resultSink = new SinkTask();
        TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
        taskGraphBuilder.addSource("source", readSource, parallism);
        ComputeConnection computeConnection = taskGraphBuilder.addSink("sink", resultSink, 1);
        computeConnection.reduce("source", "edge", Op.SUM, DataType.DOUBLE);

        DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
        ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
        taskExecutor.execute(dataFlowTaskGraph, executionPlan);
    }

    protected class SinkTask extends BaseSink {
        private static final long serialVersionUID = -254264903510284798L;

        public boolean execute(IMessage message) {
            return false;
        }
    }
}
