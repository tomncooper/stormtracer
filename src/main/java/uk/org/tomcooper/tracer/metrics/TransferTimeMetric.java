package uk.org.tomcooper.tracer.metrics;

import org.apache.storm.metric.api.IMetric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This custom metric will record the transfer time between components (and their tasks) of a Storm topology.
 * These are reported via Storm's in-built metrics system.
 *
 */
public class TransferTimeMetric implements IMetric {

     private Map<Integer, List<Long>> latencies;

    /**
     * Constructor for the transfer time custom Storm metric.
     */
    public TransferTimeMetric() {
        this.latencies = new HashMap<Integer, List<Long>>();
    }


    /**
     * Adds a transfer latency measurement to the latency list for the supplied taskID. If the supplied taskID does not
     * have an entry in the latencies map, one will be created.
     *
     * @param taskID An integer for the task latency list this measurement should be added too
     * @param ts A long representing a latency measurement in milliseconds
     */
    public void addLatency(Integer taskID, long ts){

        if(latencies.containsKey(taskID)) {
            latencies.get(taskID).add(ts);
        } else {
            ArrayList<Long> taskList = new ArrayList<Long>();
            taskList.add(ts);
            latencies.put(taskID, taskList);
        }
    }

    /**
     * Implementation of the {@link IMetric#getValueAndReset()} method.
     *
     * This averages the latency list for each source task in the latencies map and creates a new map linking
     * from the taskID to the average latency. This method will replace each task latency list with an empty list.
     *
     * @return A map linking an Integer taskID to an average latency Double.
     */
    public Object getValueAndReset() {

        Map<Integer, Double> averageLatencies = new HashMap<Integer, Double>();

        for(Map.Entry<Integer, List<Long>> e: latencies.entrySet()) {

            long sum = 0L;
            for (long latency : e.getValue()) {
                sum += latency;
            }

            Double averageLatency = Double.valueOf(sum) / Double.valueOf(latencies.size());

            if (averageLatency < 0.0) {
                averageLatency = 0.0;
            }

            //Add the average latency for this task to the results map
            averageLatencies.put(e.getKey(), averageLatency);

            //Reset the latency list for this task, ready for new measurements
            latencies.put(e.getKey(), new ArrayList<Long>());
        }
        return averageLatencies;
    }
}
