package com.epam.rd.autotasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        Runnable customRunnable = () -> {
            try {
                r.run();
            } catch (Exception throwable) {
                        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> {
            threadList.add(new FinishedThreadResult(t.getName(), e));
        });
            } finally {
                threadList.add(new FinishedThreadResult(Thread.currentThread().getName(), new Throwable()));
            }
        };
        return new Thread(customRunnable, threadName);
    }
}
