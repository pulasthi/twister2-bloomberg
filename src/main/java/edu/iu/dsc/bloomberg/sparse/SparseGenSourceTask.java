package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Logger;

public class SparseGenSourceTask extends BaseSource {
    private static final Logger LOG = Logger.getLogger(SparseGenSourceTask.class.getName());

    private String edge;
    private String inputDir;
    private double min;
    private double max;
    private String filePrefirx = "part_";
    private double sum;
    private double count;
    int row, col, sdist;
    double score, dist;
    double countx = 0;
    Integer key;
    String line;
    String splits[];
    int[] vals = new int[2];
    BufferedReader bf;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        int fileIndex = context.taskIndex();
        String fileId = (fileIndex < 100) ? "0" : "";
        fileId += (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
        String filePath = inputDir + filePrefirx + fileId;
        try {
            bf = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Worker " + context.getWorkerId() + " Task " + context.taskIndex());
        LOG.info("Starting to read file " + context.getWorkerId());
    }

    public SparseGenSourceTask(String edge, String inputDir, double min, double max) {
        this.edge = edge;
        this.inputDir = inputDir;
        this.max = max;
        this.min = min;
    }

    @Override
    public void execute() {
        try {
            line = bf.readLine();
            if (line != null && count < 2000001) {
                count++;
                splits = line.split("\\s+");
                row = Integer.valueOf(splits[0]);
                col = Integer.valueOf(splits[1]);
                score = Double.valueOf(splits[2]);
                dist = (1 / score - 1 / max) * min * max / (max - min);
                sdist = (int) (dist * Integer.MAX_VALUE);
                if (context.getWorkerId() == 0 && count % 2000000 == 0) {
                    countx++;
                    LOG.info("" + countx);
                }
                if (row > col) {
                    key = col;
                    vals[0] = row;
                    vals[1] = sdist;
                } else {
                    key = row;
                    vals[0] = col;
                    vals[1] = sdist;
                }
                context.write(this.edge, key, vals);
            } else {
                bf.close();
                LOG.info("Done readning " + context.getWorkerId());
                context.end(this.edge);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
