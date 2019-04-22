package edu.iu.dsc.bloomberg.gensparse;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.*;
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
    private static ByteOrder endianness = ByteOrder.LITTLE_ENDIAN;


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
        int chunkSize = 10000000;
        int currChuCount = 0;
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
            List<Integer> outData = new ArrayList();
            List<Integer> outIndex = new ArrayList();
            int[] outputdata = new int[chunkSize];
            int[] outputindex = new int[chunkSize*2];
            ByteBuffer outbyteBufferdata =
                    ByteBuffer.allocate(outputdata.length * 4);
            ByteBuffer outbyteBufferindex =
                    ByteBuffer.allocate(outputindex.length * 4);
            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                outbyteBufferdata.order(ByteOrder.BIG_ENDIAN);
                outbyteBufferindex.order(ByteOrder.BIG_ENDIAN);
            } else {
                outbyteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
                outbyteBufferindex.order(ByteOrder.LITTLE_ENDIAN);
            }

            while ((line = bf.readLine()) != null) {
                splits = line.split("\\s+");
                row = Integer.valueOf(splits[0]);
                col = Integer.valueOf(splits[1]);
                val = Integer.valueOf(splits[2]);
                newrow = allvals[row];
                newcol = allvals[col];

                outData.add(val);
                outIndex.add(newrow);
                outIndex.add(newcol);
                currChuCount++;
                if(currChuCount >= chunkSize){
                    //Flush the values

                    for (int i = 0; i < outputdata.length; i++) {
                        outputdata[i] = outData.get(i);
                    }

                    for (int i = 0; i < outputindex.length; i++) {
                        outputindex[i] = outIndex.get(i);
                    }
                    outbyteBufferdata.clear();
                    outbyteBufferindex.clear();

                    IntBuffer intdataOutputBuffer =
                            outbyteBufferdata.asIntBuffer();
                    intdataOutputBuffer.put(outputdata);

                    IntBuffer intOutputBuffer = outbyteBufferindex.asIntBuffer();
                    intOutputBuffer.put(outputindex);

                    outIndexfile.write(outbyteBufferindex);
                    outDatafile.write(outbyteBufferdata);
                    outData = new ArrayList();
                    outIndex = new ArrayList();
                    currChuCount = 0;
                }
            }

            if(line == null && outData.size() > 0){
                outputdata = new int[outData.size()];
                for (int i = 0; i < outputdata.length; i++) {
                    outputdata[i] = outData.get(i);
                }

                outputindex = new int[outIndex.size()];
                for (int i = 0; i < outputindex.length; i++) {
                    outputindex[i] = outIndex.get(i);
                }

                outbyteBufferdata =
                        ByteBuffer.allocate(outputdata.length * 4);
                outbyteBufferindex =
                        ByteBuffer.allocate(outputindex.length * 4);
                if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                    outbyteBufferdata.order(ByteOrder.BIG_ENDIAN);
                    outbyteBufferindex.order(ByteOrder.BIG_ENDIAN);
                } else {
                    outbyteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
                    outbyteBufferindex.order(ByteOrder.LITTLE_ENDIAN);
                }
                outbyteBufferdata.clear();
                outbyteBufferindex.clear();

                IntBuffer intdataOutputBuffer =
                        outbyteBufferdata.asIntBuffer();
                intdataOutputBuffer.put(outputdata);

                IntBuffer intOutputBuffer = outbyteBufferindex.asIntBuffer();
                intOutputBuffer.put(outputindex);

                outIndexfile.write(outbyteBufferindex);
                outDatafile.write(outbyteBufferdata);
            }

            outIndexfile.close();
            outDatafile.close();
            LOG.info("Done creating bin file");
        } catch (IOException e) {
            e.printStackTrace();
        }


        context.write(this.EDGE, new int[]{12});
        context.end(this.EDGE);
    }
}

