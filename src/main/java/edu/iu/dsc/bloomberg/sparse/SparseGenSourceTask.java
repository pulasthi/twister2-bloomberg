package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
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
    private Random random;


    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
        int fileIndex = ctx.taskIndex();
        String fileId = (fileIndex < 100) ? "0" : "";
        fileId += (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
        String filePath = inputDir + filePrefirx + fileId;
        try {
            bf = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.random = new Random();

        LOG.info("Worker " + ctx.getWorkerId() + " Task " + ctx.taskIndex());
        LOG.info("Starting to read file " + ctx.getWorkerId());
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
            int tempc = 0;
            if (count > 10000) {
                bf.close();
                LOG.info("Done readning " + context.getWorkerId());
                context.end(this.edge);
                return;
            }
            while ((line = bf.readLine()) != null && tempc < 1000) {
                count++;
                tempc++;
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
                    //vals[1] = sdist;
                    vals[1] = (int) (score * 10000);
                } else {
                    key = row;
                    vals[0] = col;
                    //vals[1] = sdist;
                    vals[1] = (int) (score * 10000);
                }
                    key = this.random.nextInt(3000000);
                    vals[0] = 233;
                    vals[1] = 123;
                if (key == 1) {
                    LOG.info(key + " " + vals[0] + " " + vals[1]);
                }
                context.write(this.edge, key, vals);
            }
            if (line == null) {
                bf.close();
                LOG.info("Done readning " + context.getWorkerId());
                context.end(this.edge);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
