package uk.org.tomcooper.tracer.metrics;

import org.apache.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tom on 22/09/16.
 */
public class TransferCountMetric implements IMetric {

    private Map<Integer, Integer> totalCounts;

    public TransferCountMetric() {
        totalCounts = new HashMap<Integer, Integer>();
    }

    public void addCount(int sourceTaskID){

        if(totalCounts.containsKey(sourceTaskID)) {
            totalCounts.put(sourceTaskID, (totalCounts.get(sourceTaskID) + 1));
        } else {
            totalCounts.put(sourceTaskID, 1);
        }
    }

    @Override
    public Object getValueAndReset() {

        Map<Integer, Integer> totals = new HashMap<Integer, Integer>();

        for(Map.Entry<Integer, Integer> e: totalCounts.entrySet()) {

            //Add the count for this task to the results map
            totals.put(e.getKey(), e.getValue().intValue());

            //Reset the count for this task, ready for new measurements
            totalCounts.put(e.getKey(), 0);
        }
        return totals;
    }
}
