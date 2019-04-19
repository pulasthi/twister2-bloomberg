package edu.iu.dsc.bloomberg.reindex;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;


public class ReIndexSource extends BaseSource {
    private static final Logger LOG = Logger.getLogger(ReIndexSource.class.getName());
    private String EDGE;

    String filePrefix = "";
    BufferedReader bf;
    String splits[];


    public ReIndexSource(String edge) {
        this.EDGE = edge;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
        int fileIndex = ctx.taskIndex();
        filePrefix = "/scratch_hdd/bloomberg/part_" + fileIndex + "_merg_sorted_summed";
    }

    @Override
    public void execute() {
        String line;
        int[] allvals = new int[31566427 + 1];

        int row;
        int col;
        int val;
        try {

            String file = filePrefix;
            bf = new BufferedReader(new FileReader(file));
            while ((line = bf.readLine()) != null) {
                splits = line.split("\\s+");
                row = Integer.valueOf(splits[0]);
                col = Integer.valueOf(splits[1]);
                allvals[row] = 1;
                allvals[col] = 1;
            }

            bf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        context.write(this.EDGE, allvals);
        context.end(this.EDGE);
    }
}

