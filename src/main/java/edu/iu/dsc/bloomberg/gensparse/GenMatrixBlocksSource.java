package edu.iu.dsc.bloomberg.gensparse;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;


public class GenMatrixBlocksSource extends BaseSource {
    private static final Logger LOG = Logger.getLogger(GenMatrixBlocksSource.class.getName());
    private String EDGE;

    String filePrefix = "";
    String indexFile = "";
    BufferedReader bf;
    String splits[];
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;


    public GenMatrixBlocksSource(String edge) {
        this.EDGE = edge;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
        int fileIndex = ctx.taskIndex();
        indexFile = "/N/u/pulasthiiu/git/pulasthi/twister2-bloomberg/bin/index_allocation.txt";
        filePrefix = "/scratch_hdd/bloomberg/part_" + fileIndex;
    }

    @Override
    public void execute() {
        String line;
        int[] allvals = new int[31566498 + 1];
        int row;
        int newrow;
        int col;
        int newcol;
        int val;
        try {

            bf = new BufferedReader(new FileReader(indexFile));
            while ((line = bf.readLine()) != null) {
                splits = line.split("\\s+");
                row = Integer.valueOf(splits[0]);
                col = Integer.valueOf(splits[1]);
                allvals[row] = col;
            }
            bf.close();

            String outFileIndex = filePrefix + "_In.bin";
            String outFiledata = filePrefix + "_Da.bin";
            FileChannel outIndexfile =
                    new FileOutputStream(outFileIndex).getChannel();

            FileChannel outDatafile =
                    new FileOutputStream(outFiledata).getChannel();
            Buffer bufferdata = null;
            Buffer bufferweight = null;
            //Read data file
            bf = new BufferedReader(new FileReader(filePrefix + "_merg_sorted_summed"));
            List<Short> outData = new ArrayList();
            List<Integer> outIndex = new ArrayList();

            while ((line = bf.readLine()) != null) {
                splits = line.split("\\s+");
                row = Integer.valueOf(splits[0]);
                col = Integer.valueOf(splits[1]);
                val = Integer.valueOf(splits[2]);
                newrow = allvals[row];
                newcol = allvals[col];
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        context.write(this.EDGE, allvals);
        context.end(this.EDGE);
    }
}

