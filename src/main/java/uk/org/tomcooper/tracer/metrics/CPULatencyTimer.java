package uk.org.tomcooper.tracer.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class CPULatencyTimer {
    /**
     * This is a helper class to allow bolts to easily track the CPU latency for their execute methods
     */
    private ThreadMXBean mxBean;
    private long threadID;
    private long startTime;

    public CPULatencyTimer(){
        mxBean = ManagementFactory.getThreadMXBean();

        if (!mxBean.isCurrentThreadCpuTimeSupported()) {
            throw new UnsupportedOperationException("CPU Latency measurements are not supported");
        }

        if (!mxBean.isThreadCpuTimeEnabled()) {
        	mxBean.setThreadCpuTimeEnabled(true);
        }
    }

    /**
     * Initialise a timer by storing the current CPU time for the supplied thread id.
     * @param threadID The id number for the thread that is to be timed
     */
    public void startTimer(long threadID){
        this.threadID = threadID;
        startTime = mxBean.getThreadCpuTime(threadID);
    }

    /**
     * Stops the running timer for the thread defined by the last call to the {@link this.startTimer} method and returns
     * the elapsed CPU time in milliseconds.
     * @return The elapsed CPU time in milliseconds.
     */
    public long stopTimer(){
        long stopTime = mxBean.getThreadCpuTime(threadID);
        long nanoDuration = stopTime - startTime;

        // Convert the duration to milliseconds
        return (nanoDuration / 1000000L);
    }
}

