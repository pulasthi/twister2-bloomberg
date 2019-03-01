package edu.iu.dsc.bloomberg;

import edu.iu.dsc.tws.task.api.BaseSource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataReadSourceTask extends BaseSource {
    private String edge;
    private String inputDir;
    private String filePrefirx = "part_";
    private double sum;
    private double count;
    public DataReadSourceTask(String edge, String inputDir) {
        this.edge = edge;
        this.inputDir = inputDir;
    }

    public void execute() {
        try {
            int currentMax = -1;
            int fileIndex = context.taskIndex();
            String fileId = (fileIndex < 100) ? "0" : "";
            fileId += (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
            String filePath = inputDir + filePrefirx + fileId;
            BufferedReader bf = new BufferedReader(new FileReader(filePath));
            String line = null;
            String splits[];

            while (count < 40000000 && (line = bf.readLine()) != null) {
                splits = line.split("\\s+");
                int row = Integer.valueOf(splits[0]);
                int col = Integer.valueOf(splits[1]);
                double value = Double.valueOf(splits[2]);
                sum += value;

                count++;
                if (context.getWorkerId() == 0 && (count % 20000000 == 0)) {
                    System.out.print(".");
                }
                if (currentMax > row) {
                    throw new IllegalStateException("File : " + fileId + " not in order at line : " + count);
                }
                currentMax = row;
            }

        }catch (IOException e){
            e.printStackTrace();
        }
    }
}