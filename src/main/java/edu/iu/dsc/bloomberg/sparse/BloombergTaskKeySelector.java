package edu.iu.dsc.bloomberg.sparse;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.TaskKeySelector;

public class BloombergTaskKeySelector implements TaskKeySelector {
    @Override
    public Object select(Object data) {
        throw new IllegalStateException("Should not be called");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {

    }
}
