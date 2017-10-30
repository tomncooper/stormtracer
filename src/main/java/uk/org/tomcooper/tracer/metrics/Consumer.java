package uk.org.tomcooper.tracer.metrics;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Thomas Cooper
 * @version 0.4
 * @date 2017/10/30
 *
 * Forwards all metrics (of interest to the tracer modelling system) to an
 * InfluxDB instance.
 */
public class Consumer implements IMetricsConsumer {

    /** InfluxDB client instance for connection to the time series database */
    private transient InfluxDB influxDB;
    /** The name of the database in InfluxDB that metrics will be sent to */
    private transient String dbName;
    /** The unique name storm has given to the topology this consumer is
     * monitoring */
    private transient String topologyID;
    /** The Logger instance for this metric consumer */
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    /**
     * Method for preparing the Metric Consumer bolt. This sets up the InfluxDB
     * connection client.
     *
     * @param stormConf Storm configuration map. This should contain the
     *                  following keys: tracerdb.host
     *                  (in the form http://hostname:port), tracerdb.name,
     *                  tracerdb.user, tracerdb.password.
     * @param registrationArgument Optional arguments for the Metrics Bolt
     * @param context The topology context object.
     * @param errorReporter The error reporting object.
     */
    public void prepare(Map stormConf, Object registrationArgument,
                        TopologyContext context, IErrorReporter errorReporter) {

        LOG.info("Setting up Influx Connection");

        String dbHost = (String) stormConf.get("tracerdb.host");
        dbName = (String) stormConf.get("tracerdb.name");
        String dbUser = (String) stormConf.get("tracerdb.user");
        String dbPwd = (String) stormConf.get("tracerdb.password");

        topologyID = (String) stormConf.get("storm.id");

        influxDB = InfluxDBFactory.connect(dbHost, dbUser, dbPwd);

        LOG.info("Influx connection object created");
    }

    /**
     * This is an implementation of the
     * {@link org.apache.storm.metric.api.IMetricsConsumer#handleDataPoints(
     * TaskInfo, Collection)} method.
     *
     * It will unpack and forward all relevant metrics to an influxDB database.
     * It has logic for both storm standard
     * metrics and the custom metrics required by the tracer system (such as
     * {@link TransferTimeMetric}).
     *
     * @param taskInfo The taskInfo instance containing details of the task that
     *                 produced this metric.
     * @param dataPoints The collection of metrics measurements provided by
     *                   Storm.
     */
    public void handleDataPoints(TaskInfo taskInfo,
                                 Collection<DataPoint> dataPoints) {


        BatchPoints batchPoints = BatchPoints.database(dbName)
                .tag("host", taskInfo.srcWorkerHost)
                .tag("workerProcess",
                        String.valueOf(taskInfo.srcWorkerPort))
                .tag("taskID", String.valueOf(taskInfo.srcTaskId))
                .tag("component", taskInfo.srcComponentId)
                .tag("topology", topologyID)
                .consistency(InfluxDB.ConsistencyLevel.ANY)
                .retentionPolicy("autogen")
                .build();

        long timestamp = taskInfo.timestamp;

        for (DataPoint p : dataPoints) {

            if(p.value instanceof Map){
                // Some of the metrics have values that are maps for sub metrics
                // of the main metric.
                Map dataMap = (Map) p.value;

                //Some of the metrics maps can be empty so check they have
                // values first
                if(!dataMap.isEmpty()){

                    LOG.debug("Preparing batch points for metric: " +
                              p.name);

                    if(p.name.contains("count") | p.name.contains("latency")){
                        //The count and latency metrics contain keys for each
                        // stream add a separate point for each stream and
                        // value pair

                        Map<String, Object> datamap =
                                (Map<String, Object>) dataMap;

                        for(Map.Entry<String, Object> e : datamap.entrySet()) {

                            Point point = Point.measurement(
                                    p.name.replaceAll("__", ""))
                                    .time(timestamp, TimeUnit.SECONDS)
                                    .tag("stream", e.getKey())
                                    .addField("value",
                                              (Number) e.getValue())
                                    .build();

                            //Add the point to this tasks batch
                            batchPoints.point(point);

                        }

                    } else if(p.name.equals("__sendqueue") |
                              p.name.equals("__receive") |
                              p.name.equals("__transfer")){
                        //The executor send & receive queues and the worker
                        // transfer queue metrics contain a map metrics for each
                        // queue

                       Map<String, Object> datamap =
                               (Map<String, Object>) dataMap;

                        for(Map.Entry<String, Object> e : datamap.entrySet()) {

                            Point point = Point.measurement(
                                    p.name.replaceAll("__", "") +
                                                      "." + e.getKey())
                                    .time(timestamp, TimeUnit.SECONDS)
                                    .addField("value",
                                              (Number) e.getValue())
                                    .build();

                            //Add the point to this tasks batch
                            batchPoints.point(point);

                        }

                    } else if(p.name.contains("Transfer-")){

                        // The key for this metric is a string of the form:
                        // Transfer-MetricType-SourceComponent
                        // -StreamID-DestinationComponent
                        // The value (p.value) is a map from an integer taskID
                        // to a Double or Integer representing the average
                        // transfer latency or count.

                        // Convert dataMap to the correct Map type.
                        Map<Integer, Object> datamap =
                                (Map<Integer, Object>) dataMap;

                        // Extract the tag information for this
                        // measurement
                        Map<String, String> tags = extractTags(p.name);

                        //Loop over all the tasks in this metric and create a
                        // point of each
                        for(Map.Entry<Integer, Object> e : datamap.entrySet()) {

                            String sourceTask = String.valueOf(e.getKey());
                            Number value;

                            if(tags.get("measurement").equals
                                    ("Transfer-Latency")){
                                value = (Double) e.getValue();
                                if(Double.isNaN((Double) value)){
                                    break;
                                }
                            } else if(tags.get("measurement").equals
                                    ("Transfer-Count")){
                                value = (Integer) e.getValue();
                            } else {
                                throw new RuntimeException("Transfer " +
                                        "metric " + tags.get("measurement")
                                        + " is not supported.");
                            }

                            Point point = Point.measurement(
                                    tags.get("measurement"))
                                    .time(timestamp, TimeUnit.SECONDS)
                                    .tag("source-task", sourceTask)
                                    .tag("source-component",
                                            tags.get("source-component"))
                                    .tag("stream",
                                            tags.get("stream"))
                                    .addField("value", value)
                                    .build();

                            //Add the point to this tasks batch
                            batchPoints.point(point);
                        }

                    } else {
                        // This is likely one of the resource metrics. Append
                        // the sub metric name to the main metric and send it on.

                        Map<String, Object> datamap =
                                (Map<String, Object>) dataMap;

                        for(Map.Entry<String, Object> e : datamap.entrySet()) {

                            Number value = 0;
                            try{
                                value = (Number) e.getValue();
                            } catch (ClassCastException exp) {
                                LOG.warn("Value of sub-metric " + e.getKey()
                                         + " of metric " + p.name
                                         + "was a " + e.getValue().getClass()
                                         + " not a number");
                                break;
                            }

                            Point point = Point.measurement(
                                    p.name + "." + e.getKey())
                                    .time(timestamp, TimeUnit.SECONDS)
                                    .addField("value", value)
                                    .build();

                            //Add the point to this tasks batch
                            batchPoints.point(point);

                        }
                    }

                } else {
                    //Empty Map value - Which is odd?
                    LOG.warn("Empty map value during " + p.name +
                             " metric point handling");
                }

            } else if(p.value instanceof Number) {

                //Some of the metrics are just numbers and so can just be
                // entered as straight measurements
                Point point = Point.measurement(
                        p.name.replaceAll("__", ""))
                        .time(timestamp, TimeUnit.SECONDS)
                        .addField("value", (Number) p.value)
                        .build();

                batchPoints.point(point);
            }

        }

        try {
            //Write the batch of points for this tasks metrics to InfluxDB.
            influxDB.write(batchPoints);
            LOG.info(batchPoints.getPoints().size() + " metrics sent to " +
                    "InfluxDB with timestamp: " + timestamp);
        } catch (NullPointerException n) {
            LOG.warn("Sending to InfluxDB raised a NullPointer Exception");
            n.printStackTrace();
        } catch (Exception e) {
            LOG.warn("Sending to InfluxDB raised a generic exception");
            for(Point point : batchPoints.getPoints())
                System.err.println(point);
            e.printStackTrace();
        }
    }

    /**
    * This will extract the measurement name, source component and stream
     * from the supplied transfer metric name.
     *
     * @param transferName The metric name to extract information from. This
     *                    will be in the form:
     *                     Transfer-MetricType-SourceComponent-StreamID
     *                     -DestinationComponent.
     *
     * @return A Map linking the measurement name, source-component and
     * stream to the respective values.
    */
    private Map<String, String> extractTags(String transferName){
       String[] tokens = transferName.split("-");
       Map <String, String> tags = new HashMap<String, String>();

       tags.put("measurement", "Transfer-" + tokens[1]);
       tags.put("source-component", tokens[2]);
       tags.put("stream", tokens[3]);

       return tags;
    }

    public void cleanup() {
        influxDB.close();
    }
}
