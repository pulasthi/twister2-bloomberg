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
    long countdoubles = 0;
    String filePath = "/scratch_hdd/bloomberg/";


    Map<Integer, TreeMap<Integer, Integer>> data;

    public SortDataSource(String edge) {
        this.EDGE = edge;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
        int fileIndex = ctx.taskIndex();
        filePrefix = "/scratch_hdd/bloomberg/part_" + filePrefix + "__";
        data = new HashMap<>();
    }

    @Override
    public void execute() {
        String line = null;
        int row;
        int col;
        int val;
        for (int i = 0; i < rounds; i++) {
            String file = filePrefix + i;
            try {
                bf = new BufferedReader(new FileReader(file));

                while ((line = bf.readLine()) != null) {
                    splits = line.split("\\s+");
                    row = Integer.valueOf(splits[0]);
                    col = Integer.valueOf(splits[1]);
                    val = Integer.valueOf(splits[2]);

                    if (data.containsKey(row)) {
                        TreeMap<Integer, Integer> temp = data.get(row);
                        if (temp.containsKey(col)) {
                            temp.put(col, (temp.get(col) + val) / 2);
                            countdoubles++;
                        } else {
                            temp.put(col, val);
                        }
                    } else {
                        TreeMap<Integer, Integer> temp = new TreeMap<>();
                        temp.put(col, val);
                        data.put(row, temp);
                    }
                }
                bf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //Now sort and print
        Integer[] sorted = (Integer[])data.keySet().toArray();
        Arrays.sort(sorted);
        try {
            PrintWriter outWriter = new PrintWriter(new FileWriter(filePath + "sorted_part_" + context.taskId()));

            for (Integer sortedrow : sorted) {
                TreeMap<Integer, Integer> temp = data.get(sortedrow);
                for (Map.Entry<Integer, Integer> entry : temp.entrySet()) {
                    outWriter.println(sortedrow + " " + entry.getKey() + " " + entry.getValue());
                }
            }
            outWriter.flush();
            outWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


        LOG.info("Count doubles " + countdoubles);
        context.write(this.EDGE, countdoubles);
        context.end(this.EDGE);
    }
}
