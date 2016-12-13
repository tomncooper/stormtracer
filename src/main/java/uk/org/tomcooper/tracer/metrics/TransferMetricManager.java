package uk.org.tomcooper.tracer.metrics;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for managing multiple tracer latencies objects. This is used to handle components with more than one
 * input source component/stream, so that the individual transfer latencies between the different tasks for each component
 * can be tracked.
 *
 * @author Thomas Cooper
 * @version 0.2
 * @date 22/09/16
 */
public class TransferMetricManager {

    /**
     * Map containing the TransferTimeMetrics for each incoming stream to the component creating this manager object
     **/
    private transient Map<GlobalStreamId, TransferTimeMetric> latencies;

    /**
     * Map containing the TransferCountMetrics for each incoming stream to the component creating this manager object
     **/
    private transient Map<GlobalStreamId, TransferCountMetric> counts;

    /**
     * The length, in seconds, between the calls to the getValueAndReset() method for each of the latencies stored in this manager
     **/
    private int metricWindow;


    /**
     * Constructor for the Transfer Metric Manager class. This will take the {@link org.apache.storm.task.TopologyContext} object provided by a Bolt's
     * prepare method and will register various transfer metric objects for each incoming stream into that bolt.
     *
     * Each Transfer Metric is registered with the supplied {@link org.apache.storm.task.TopologyContext} object using a name in the form:
     *
     * "Transfer-{Latency/Count}-</><SourceComponentID>-<StreamID>-<DestinationComponentID>".
     *
     * @param topoConfig  A Map, supplied to a bolt's prepare method, containing the configuration for this Storm topology.
     * @param topoContext A {@link org.apache.storm.task.TopologyContext} object for the component creating this latencies manager.
     */
    public TransferMetricManager(Map topoConfig, TopologyContext topoContext) {

        latencies = new HashMap<GlobalStreamId, TransferTimeMetric>();
        counts = new HashMap<GlobalStreamId, TransferCountMetric>();

        //Set the reporting window for the latencies to be the same as the system latencies window
        Long mWin = (Long) topoConfig.get("topology.builtin.metrics.bucket.size.secs");
        metricWindow = mWin.intValue();

        //Create the required number of transfer latencies to match the incoming connections to this component
        createMetrics(topoContext);
    }

    /**
     * This private method will create metrics (to be registered with Storm) from the supplied {@link org.apache.storm.task.TopologyContext} object.
     * The names of these latencies take the form: "Transfer-{Latency/Count}-<SourceComponent>-<Stream>-<DestinationComponent>".
     *
     * @param tc A {@link org.apache.storm.task.TopologyContext} object for the component creating this latencies manager.
     */
    private void createMetrics(TopologyContext tc) {

        for (Map.Entry entry : tc.getThisSources().entrySet()) {
            GlobalStreamId gsid = (GlobalStreamId) entry.getKey();

            //Form the name string for this metric
            String metricName = gsid.get_componentId() + "-" + gsid.get_streamId() + "-" + tc.getThisComponentId();
            String ttmName = "Transfer-Latency-" + metricName;
            String tcmName = "Transfer-Count-" + metricName;

            //Create the transfer metric objects for this source
            TransferTimeMetric ttm = new TransferTimeMetric();
            TransferCountMetric tcm = new TransferCountMetric();

            //Register the transfer latencies with the topology
            tc.registerMetric(ttmName, ttm, metricWindow);
            tc.registerMetric(tcmName, tcm, metricWindow);

            //Add the metric instance to the manger's metric maps
            latencies.put(gsid, ttm);
            counts.put(gsid, tcm);
        }

    }

    /**
     * Adds a transfer time latency measurement and increments the count of the corresponding transfer metric objects stored in this manager.
     * The {@link GlobalStreamId} from the supplied {@link Tuple} object is used to locate the correct metric in the manager's maps.
     *
     * @param tuple   The {@link Tuple} object supplied to the bolt's execute method.
     * @param latency A measurement of transfer time between components. This should be calculated in the execute method using
     *                System.currentTimeMillis() minus the tuple's timestamp field.
     */
    public void addTransfer(Tuple tuple, long latency) {
        latencies.get(tuple.getSourceGlobalStreamId()).addLatency(tuple.getSourceTask(), latency);
        //TODO This assumes that every tuple will report a transfer. This needs to be edited to take the sampling
        //rate into consideration
        counts.get(tuple.getSourceGlobalStreamId()).addCount(tuple.getSourceTask());
    }

}
