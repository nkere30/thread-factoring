package com.epam.rd.autotasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadUnionImpl implements ThreadUnion{

    private final String name;
    private final AtomicInteger threadCount = new AtomicInteger(0);
    private final List<FinishedThreadResult> threadList = Collections.synchronizedList(new ArrayList<>());
    private boolean shutdownRequested = false;
    public ThreadUnionImpl(String name) {
        this.name = name;
    }
    @Override
    public int totalSize() {
        return threadCount.get();
    }

    @Override
    public int activeSize() {
        return (int)Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(name + "-worker-"))
                .count();
    }

    @Override
    public void shutdown() {
        shutdownRequested = true;
        Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(name + "-worker-"))
                .forEach(Thread::interrupt);
        Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(name + "-worker-"))
                .forEach(Thread::interrupt);
    }

    @Override
    public boolean isShutdown() {
        return shutdownRequested;
    }

    @Override
    public void awaitTermination() {
        Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith(name + "-worker-"))
                .forEach(thread -> {
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return shutdownRequested && activeSize() == 0;
    }

    @Override
    public List<FinishedThreadResult> results() {
        return threadList;
    }

    @Override
    public Thread newThread(Runnable r) {
        if (shutdownRequested) throw new IllegalStateException();
        String threadName = this.name + "-worker-" + threadCount.getAndIncrement();
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        return new Thread(() -> {
            try {
                r.run();
            } catch (Throwable throwable) {
                // Upon catching an exception, store the result with the thrown exception
                exceptionThrown.set(true);
                threadList.add(new FinishedThreadResult(Thread.currentThread().getName(), throwable));
            } finally {
                // After execution, store the result with null Throwable
                if(!exceptionThrown.get())
                    threadList.add(new FinishedThreadResult(Thread.currentThread().getName(), null));
            }
        }, threadName);
    }
}
