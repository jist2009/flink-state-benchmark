package com.benchmark;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class PersonSource extends RichParallelSourceFunction<Person> {
    private volatile boolean isRunning = true;
    private static final long RUN_DURATION_MS = 300_010L; 
    
    @Override
    public void run(SourceContext<Person> ctx) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        long id = 1000 + subtask;
        long startTime = System.currentTimeMillis();

        while (isRunning && (System.currentTimeMillis() - startTime < RUN_DURATION_MS)) {
            long ts = System.currentTimeMillis();
            ctx.collect(new Person(id++, "User_" + id, ts));
            id += totalSubtasks;
            Thread.sleep(10); // génère ~1000 événements/sec pour le test local
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}