package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.task.api.TaskPartitioner;

import java.util.*;

public class BloombergPartitioner implements TaskPartitioner {
    int total = 31600000;
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
    }

    @Override
    public int partition(int source, Object data) {
        int key = (Integer) data;
        int part = (int) Math.floor((double) key / perTask);
        return destination.get(part);
    }

    @Override
    public void commit(int source, int partition) {

    }
}
