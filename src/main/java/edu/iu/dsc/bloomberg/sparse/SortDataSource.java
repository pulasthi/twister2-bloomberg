package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.Edge;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;


public class SortDataSource extends BaseSource {
    private static final Logger LOG = Logger.getLogger(SortDataSource.class.getName());
    private String EDGE;

    String filePrefix = "";
    int rounds = 10;
    BufferedReader bf;
    String splits[];
    double countdoubles = 0;
    String filePath = "/scratch_hdd/bloomberg/";


    Map<Integer, TreeMap<Integer, Integer>> data;
//    Map<Integer, Map<Integer, Integer>> dupCounts;

    public SortDataSource(String edge) {
        this.EDGE = edge;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
        int fileIndex = ctx.taskIndex();
        filePrefix = "/scratch_hdd/bloomberg/part_" + fileIndex + "_merg_sorted";
        data = new HashMap<>();
//        dupCounts = new HashMap<>();
    }

    @Override
    public void execute() {
        String line = null;
        int prow = 0, pcol = 0, pval = 0;
        boolean readFirst = false;


        int row;
        int col;
        int val;
        try {
            PrintWriter outWriter = new PrintWriter(new FileWriter(filePrefix + "_summed"));

            String file = filePrefix;
            bf = new BufferedReader(new FileReader(file));

            while ((line = bf.readLine()) != null) {
                splits = line.split("\\s+");
                row = Integer.valueOf(splits[0]);
                col = Integer.valueOf(splits[1]);
                val = Integer.valueOf(splits[2]);
                if (!readFirst) {
                    readFirst = true;
                    continue;
                }
                if (prow == row && pcol == col) {
                    outWriter.println(row + " " + col + " " + (pval / 2 + val / 2));
                    readFirst = false;
                } else {
                    outWriter.println(prow + " " + pcol + " " + pval);
                    prow = row;
                    pcol = col;
                    pval = val;
                }
            }

            bf.close();
            outWriter.flush();
            outWriter.close();

            //LOG.info("Total number of rows " + sorted.length);
        } catch (IOException e) {
            e.printStackTrace();
        }


        context.write(this.EDGE, new double[]{countdoubles});
        context.end(this.EDGE);
    }
}
