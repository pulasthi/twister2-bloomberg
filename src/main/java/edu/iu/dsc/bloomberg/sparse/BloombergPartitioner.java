package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.task.api.TaskPartitioner;

import java.util.*;

public class BloombergPartitioner implements TaskPartitioner {
    int total = 31600000;
    int[] partArray = new int[316];
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
        for (int i = 0; i < 70; i++) {
            partArray[i] = partCount++;

        }
        for (int i = 70; i < partArray.length; i = i +2) {
            partArray[i] = partCount;
            partArray[i+1] = partCount++;
        }
    }

    @Override
    public int partition(int source, Object data) {
        int key = (Integer) data;
        int keypart = (int) Math.floor((double)key/100000);
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
