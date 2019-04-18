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
        filePrefix = "/scratch_hdd/bloomberg/part_" + fileIndex + "__";
        data = new HashMap<>();
//        dupCounts = new HashMap<>();
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
                            temp.put(col, temp.get(col) / 2 + val / 2);
                            countdoubles++;
//                            if (dupCounts.containsKey(row)) {
//                                if (dupCounts.get(row).containsKey(col)) {
//                                    dupCounts.get(row).put(col, dupCounts.get(row).get(col) + 1);
//                                } else {
//                                    dupCounts.get(row).put(col, 2);
//                                }
//                            } else {
//                                dupCounts.put(row, new HashMap<>());
//                                dupCounts.get(row).put(col, 2);
//                            }
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


//        for (Integer integer : dupCounts.keySet()) {
//            for (Map.Entry<Integer, Integer> s : dupCounts.get(integer).entrySet()) {
//                if(s.getValue() > 2){
//                    LOG.info("####### " + integer + " " + s.getKey() + " " + s.getValue());
//                }
//            }
//        }
        //Now sort and print
        LOG.info("Done Loading data");
        int[] sorted = new int[data.keySet().size()];
        Object[] keys = data.keySet().toArray();
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] = (Integer) keys[i];
        }

        Arrays.sort(sorted);
        LOG.info("Total number of rows " + sorted.length);
        try {
            PrintWriter outWriter = new PrintWriter(new FileWriter(filePath + "sorted_part_" + context.taskIndex()));

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
        context.write(this.EDGE, new double[]{countdoubles});
        context.end(this.EDGE);
    }
}
