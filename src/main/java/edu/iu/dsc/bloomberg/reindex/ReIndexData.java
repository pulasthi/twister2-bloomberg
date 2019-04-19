package edu.iu.dsc.bloomberg.reindex;

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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.logging.Logger;

public class ReIndexData extends TaskWorker {
    private static final Logger LOG = Logger.getLogger(ReIndexData.class.getName());

    @Override
    public void execute() {
        long startTime = System.currentTimeMillis();
        int parallism = 192;
        String filePath = config.getStringValue("input");
        String outFilePath = config.getStringValue("output");
        BaseSource readSource = new ReIndexSource("edge");
        BaseSink resultSink = new ReIndexData.SinkTask();

        TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
        taskGraphBuilder.setMode(OperationMode.BATCH);
        taskGraphBuilder.addSource("source", readSource, parallism);
        ComputeConnection computeConnection = taskGraphBuilder.addSink("sink", resultSink, 1);
        computeConnection.reduce("source", "edge", Op.SUM, DataType.INTEGER);
        DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
        ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
        taskExecutor.execute(dataFlowTaskGraph, executionPlan);


        LOG.info("Total time taken : " + (System.currentTimeMillis() - startTime));

    }

    protected static class SinkTask extends BaseSink {
        private static final long serialVersionUID = -254264903510284798L;
        private static final Logger LOG = Logger.getLogger(ReIndexData.SinkTask.class.getName());

        public boolean execute(IMessage message) {
            try {
                PrintWriter outWriter = new PrintWriter(new FileWriter("/scratch_hdd/bloomberg/index_allocation.txt"));
                int[] data = (int[]) message.getContent();
                int newIndex = 0;
                int missingCount = 0;
                for (int i = 1; i < data.length; i++) {
                    if (data[i] > 0) {
                        outWriter.println(data[i] + " " + newIndex);
                        newIndex++;
                    } else {
                        missingCount++;
                    }
                }
                outWriter.flush();
                outWriter.close();
                LOG.info("################ Done ################### Missing : " + missingCount);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return true;
        }
    }
}
