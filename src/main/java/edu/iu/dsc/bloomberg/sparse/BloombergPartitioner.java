package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.task.api.TaskPartitioner;

import java.util.*;

public class BloombergPartitioner implements TaskPartitioner {
    int total = 31600000;
    int[] partArray = new int[3160];
    int perTask = total / 192;
    private Map<Integer, Integer> destination = new HashMap<>();

    @Override
    public void prepare(Set<Integer> sources, Set<Integer> destinations) {
        List<Integer> sortedDest = new ArrayList<>();
        for (Integer integer : destinations) {
            sortedDest.add(integer);
        }
        Collections.sort(sortedDest);
        for (int i = 0; i < sortedDest.size(); i++) {
            destination.put(i, sortedDest.get(i));
        }
        int partCount = 0;

        for (int i = 0; i < 160; i += 5) {
            partArray[i] = partCount;
            partArray[i+1] = partCount;
            partArray[i+2] = partCount;
            partArray[i+3] = partCount;
            partArray[i+4] = partCount++;

        }
        for (int i = 160; i < 500; i += 10) {
            for (int j = 0; j < 9; j++) {
                partArray[i+j] = partCount;
            }
            partArray[i+9] = partCount++;

        }
        for (int i = 500; i < 2740; i += 20) {
            for (int j = 0; j < 19; j++) {
                partArray[i+j] = partCount;
            }
            partArray[i+19] = partCount++;
        }

        for (int i = 2740; i < 3160; i += 30) {
            for (int j = 0; j < 29; j++) {
                partArray[i+j] = partCount;
            }
            partArray[i+29] = partCount++;
        }
    }

    @Override
    public int partition(int source, Object data) {
        int key = (Integer) data;
        int keypart = (int) Math.floor((double)key/10000);
        if(key > 31600000){
            throw new IllegalStateException("out of range key");
        }
        int part = partArray[keypart];
        return destination.get(part);
    }

    @Override
    public void commit(int source, int partition) {

    }
}
