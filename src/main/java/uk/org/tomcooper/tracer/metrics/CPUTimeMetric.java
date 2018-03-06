package uk.org.tomcooper.tracer.metrics;

import org.apache.storm.metric.api.IMetric;

import java.util.ArrayList;
import java.util.List;

/**
 * This custom metric will record the CPU Execute Latency for tasks within a Storm Topology.
 * These are reported via Storm's in-built metrics system.
 *
 */
public class CPUTimeMetric implements IMetric {

    private List<Long> latencies;

    public CPUTimeMetric() { this.latencies = new ArrayList<Long>(); }

    /**
     * Adds a cpu latency measurement to the latency list.
     *
     * @param latency A long representing a latency measurement in milliseconds
     */
    public void addLatency(long latency){
        latencies.add(latency);
    }

    /**
     * Implementation of the {@link IMetric#getValueAndReset()} method.
     *
     * This averages the latency list and resets it to an empty list.
     *
     * @return A latency Double for the average CPU latency in the metric bucket period.
     */
    public Object getValueAndReset() {

        if(!latencies.isEmpty()) {
            long sum = 0L;
            for (long latency : latencies) {
                sum += latency;
            }

            Double averageLatency = Double.valueOf(sum) / Double.valueOf(latencies.size());

            //Just incase issues with CPU nanosecond reporting leads to -ve values
            if (averageLatency < 0.0) {
                averageLatency = 0.0;
            }

            //Reset the latency ready for new measurements
            latencies = new ArrayList<Long>();

            return averageLatency;

        } else {
            return 0.0;
        }
    }
}
