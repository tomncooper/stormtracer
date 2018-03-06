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

        LOG.info("Influx connection client created");
    }

    /**
     * This is an implementation of the
     * {@link org.apache.storm.metric.api.IMetricsConsumer#handleDataPoints(
     * TaskInfo, Collection)} method.
     *
     * It will unpack and forward all relevant metrics to an influxDB database.
     * It has logic for both storm standard metrics and the custom metrics
     * required by the tracer system (such as the {@link TransferTimeMetric}).
     *
     * @param taskInfo The taskInfo instance containing details of the task that
     *                 produced this metric.
     * @param dataPoints The collection of metrics measurements provided by
     *                   Storm.
     */
    public void handleDataPoints(TaskInfo taskInfo,
                                 Collection<DataPoint> dataPoints) {

        // Set up the batch with common tags for all data points
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

            String metric = p.name;

            LOG.debug("Preparing batch points for metric: " + metric);

            if(p.value instanceof Map){
                // Some of the metrics have values that are maps for sub metrics
                // of the main metric.
                Map dataMap = (Map) p.value;

                //Some of the metrics maps can be empty so check they have
                // values first
                if(!dataMap.isEmpty()){

                    if(metric.contains("count") | metric.contains("latency")){
                        handle_count_latency_metrics(metric, timestamp,
                                                     dataMap, batchPoints);
                    } else if(metric.equals("__sendqueue") |
                              metric.equals("__receive") |
                              metric.equals("__transfer")) {
                        handle_disruptor_queues(metric, timestamp,
                                                dataMap, batchPoints);
                    } else if (metric.contains("__recv-iconnection")) {
                        handle_revc_iconn_metric(metric, timestamp,
                                                 dataMap, batchPoints);
                    } else if (metric.contains("__send-iconnection")) {
                        handle_send_iconn_metric(metric, timestamp,
                                                 dataMap, batchPoints);
                    } else if(p.name.contains("Transfer-")){
                        handle_transfer_metric(metric, timestamp, dataMap,
                                               batchPoints);
                    } else if(p.name.contains("CPU-Latency")){
                        System.out.println("\n########\n\nCPU Latency is a Map!\n\n#######\n");
                    } else {
                        // This is likely one of the resource metrics.
                        handle_other(metric, timestamp, dataMap, batchPoints);
                    }

                } else {
                    //Empty Map value - Which is odd?
                    LOG.warn("Empty map value during " + metric +
                             " metric point handling");
                }

            } else if(p.value instanceof Number) {

                if(p.name.contains("CPU-Latency")) {
                    Number latency = (Number) p.value;
                    handle_CPU_metric(metric, timestamp, latency, batchPoints);
                } else {
                    //Some of the metrics values are simply numbers and so can just
                    // be entered directly as measurements
                    Point point = Point.measurement(
                            metric.replaceAll("__", ""))
                            .time(timestamp, TimeUnit.SECONDS)
                            .addField("value", (Number) p.value)
                            .build();

                    batchPoints.point(point);
                }
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
     *                     will be in the form:
     *                     Transfer-MetricType-SourceComponent-StreamID
     *                     -DestinationComponent.
     *
     * @return A Map linking the measurement name, source-component and
     * stream to the respective values.
    */
    private Map<String, String> extractTags(String transferName){
       String[] tokens = transferName.split("-");
       Map <String, String> tags = new HashMap<String, String>();

       tags.put("measurement", tokens[0] + "-" + tokens[1]);
       tags.put("source-component", tokens[2]);
       tags.put("stream", tokens[3]);

       return tags;
    }

    /**
     * This method will be run during an orderly shutdown of the topology via
     * the KILL command and will close the InfluxDB connection.
      */
    public void cleanup() {
        influxDB.close();
    }

    /**
     * Handles the various count and latency metrics for each of the task
     * instances.
     *
     * @param metric The name of the metric being handled
     * @param timestamp The timestamp to be applied to this measurement
     * @param dataMap The map of data for this metric data point
     * @param batchPoints The current instance of BatchPoints that this metric
     *                    will be added to.
     */
    private void handle_count_latency_metrics(String metric,
                                              long timestamp,
                                              Map dataMap,
                                              BatchPoints batchPoints) {
        //The count and latency metrics contain keys for each
        // stream add a separate point for each stream and
        // value pair

        Map<String, Object> datamap =
                (Map<String, Object>) dataMap;

        for (Map.Entry<String, Object> e : datamap.entrySet()) {

            Point point = Point.measurement(
                    metric.replaceAll("__", ""))
                    .time(timestamp, TimeUnit.SECONDS)
                    .tag("stream", e.getKey())
                    .addField("value",
                            (Number) e.getValue())
                    .build();

            //Add the point to this tasks batch
            batchPoints.point(point);

        }
    }

    /**
     * Handles metrics from the LMAX disruptor queues.
     *
     * @param metric The name of the metric being handled
     * @param timestamp The timestamp to be applied to this measurement
     * @param dataMap The map of data for this metric data point
     * @param batchPoints The current instance of BatchPoints that this metric
     *                    will be added to.
     */
    private void handle_disruptor_queues(String metric,
                                         long timestamp,
                                         Map dataMap,
                                         BatchPoints batchPoints) {
        //The executor send & receive queues and the worker
        // transfer queue metrics contain a map metrics for each
        // queue

        Map<String, Object> datamap =
                (Map<String, Object>) dataMap;

        for (Map.Entry<String, Object> e : datamap.entrySet()) {

            Point point = Point.measurement(
                    metric.replaceAll("__", "") +
                            "." + e.getKey())
                    .time(timestamp, TimeUnit.SECONDS)
                    .addField("value",
                            (Number) e.getValue())
                    .build();

            //Add the point to this tasks batch
            batchPoints.point(point);

        }
    }

    /**
     * Handles Storm Tracer custom metrics such as Transfer-Latency and
     * Transfer-Count.
     *
     * @param metric The name of the metric being handled
     * @param timestamp The timestamp to be applied to this measurement
     * @param dataMap The map of data for this metric data point
     * @param batchPoints The current instance of BatchPoints that this metric
     *                    will be added to.
     */
    private void handle_transfer_metric(String metric,
                                        long timestamp,
                                        Map dataMap,
                                        BatchPoints batchPoints) {
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
        Map<String, String> tags = extractTags(metric);

        //Loop over all the tasks in this metric and create a
        // point of each
        for (Map.Entry<Integer, Object> e : datamap.entrySet()) {

            String sourceTask = String.valueOf(e.getKey());
            Number value;

            if (tags.get("measurement").equals
                    ("Transfer-Latency")) {
                value = (Double) e.getValue();
                if (Double.isNaN((Double) value)) {
                    break;
                }
            } else if (tags.get("measurement").equals
                    ("Transfer-Count")) {
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
    }

    /**
     * Handles Storm Tracer custom CPU latency metrics.
     *
     * @param metric The name of the metric being handled
     * @param timestamp The timestamp to be applied to this measurement
     * @param latency The CPU latency measurement
     * @param batchPoints The current instance of BatchPoints that this metric
     *                    will be added to.
     */
    private void handle_CPU_metric(String metric,
                                   long timestamp,
                                   Number latency,
                                   BatchPoints batchPoints) {
        // The key for this metric is a string of the form:
        // CPU-Latency-SourceComponent-StreamID-DestinationComponent
        // The value (p.value) is a double representing the average
        // latency in the metric bucket period.

        // Extract the tag information for this
        // measurement
        Map<String, String> tags = extractTags(metric);

        Double value = (Double) latency;

        Point point = Point.measurement(
                tags.get("measurement"))
                .time(timestamp, TimeUnit.SECONDS)
                .tag("source-component",
                        tags.get("source-component"))
                .tag("stream",
                        tags.get("stream"))
                .addField("value", value)
                .build();

        //Add the point to this tasks batch
        batchPoints.point(point);
    }

    /**
     * Handles metrics from the Netty receive buffers.
     *
     * @param metric The name of the metric being handled
     * @param timestamp The timestamp to be applied to this measurement
     * @param dataMap The map of data for this metric data point
     * @param batchPoints The current instance of BatchPoints that this metric
     *                    will be added to.
     */
    private void handle_revc_iconn_metric(String metric,
                                          long timestamp,
                                          Map dataMap,
                                          BatchPoints batchPoints) {
        // Sub Metrics for the Worker process Netty input buffers
        // can have values which are nested hash maps or just a
        // hash map

        Map<String, Object> datamap = (Map<String, Object>) dataMap;

        for(Map.Entry<String, Object> e : datamap.entrySet()) {

            String measurement = metric
                    .replaceAll("__", "")
                    + "." + e.getKey();

            if (e.getValue() instanceof Map) {

                Map valueMap = (Map<String, Object>) e.getValue();

                for (Object ie : valueMap.entrySet()) {

                    Map.Entry<String, Object> inner =
                            (Map.Entry<String, Object>) ie;

                    if (inner.getValue() instanceof Number) {

                        String[] hostport = inner.getKey()
                                            .split("/")[1].split(":");

                        Point point = Point.measurement(measurement)
                                .time(timestamp, TimeUnit.SECONDS)
                                .tag("sending-host-ip", hostport[0])
                                .tag("sending-host-port", hostport[1])
                                .addField("value",
                                          (Number) inner.getValue())
                                .build();

                        batchPoints.point(point);

                    } else {
                        LOG.warn("Revc-iconnection inner metric "
                                + inner.getKey()
                                + " value was not a number. It"
                                + " was a "
                                + inner.getValue().getClass()
                                + ": " + inner.getValue());
                    }

                }

            } else if (e.getValue() instanceof Number) {

                Point point = Point.measurement(measurement)
                        .time(timestamp, TimeUnit.SECONDS)
                        .addField("value",
                                (Number) e.getValue())
                        .build();

                //Add the point to this tasks batch
                batchPoints.point(point);
            }

        }
    }

    /**
     * Handles metrics from the Netty send buffers.
     *
     * @param metric The name of the metric being handled
     * @param timestamp The timestamp to be applied to this measurement
     * @param dataMap The map of data for this metric data point
     * @param batchPoints The current instance of BatchPoints that this metric
     *                    will be added to.
     */
    private void handle_send_iconn_metric(String metric,
                                          long timestamp,
                                          Map dataMap,
                                          BatchPoints batchPoints) {
        // Sub Metrics for the Worker process Netty output buffers
        // has values which are nested hash maps

        Map<String, Object> datamap = (Map<String, Object>) dataMap;

        for(Map.Entry<String, Object> e : datamap.entrySet()) {

            String measurement = metric.replaceAll("__", "");

            String[] workersupervisor = e.getKey().split("/");

            if (e.getValue() instanceof HashMap){

                Map valueMap = (Map<String, Object>) e.getValue();

                // If the inner value is a hash map this map has the following
                // form:
                // reconnects=0,
                // src=/10.0.0.8:54694,
                // pending=0,
                // dest=TNC-Worker-1/10.0.0.6:6700,
                // sent=137,
                // lostOnSend=0

                // Extract source and host ip port name etc
                String[] srcHostIPPort = ((String) valueMap.get("src"))
                                         .split("/")[1]
                                         .split(":");

                String[] destHostnameIpPort = ((String) valueMap
                                               .get("dest"))
                                               .split("/");

                String[] destHostIpPort = destHostnameIpPort[1]
                                          .split(":");

                Point point = Point.measurement(measurement)
                        .time(timestamp, TimeUnit.SECONDS)
                        .tag("workerProcess", workersupervisor[0])
                        .tag("supervisor", workersupervisor[1])
                        .tag("source-host-ip", srcHostIPPort[0])
                        .tag("source-host-port", srcHostIPPort[1])
                        .tag("dest-host-name", destHostnameIpPort[0])
                        .tag("dest-host-ip", destHostIpPort[0])
                        .tag("dest-host-port", destHostIpPort[1])
                        .addField("reconnects",
                                (Number) valueMap.get("reconnects"))
                        .addField("pending",
                                (Number) valueMap.get("pending"))
                        .addField("sent",
                                  (Number) valueMap.get("sent"))
                        .addField("lostOnSend",
                                (Number) valueMap.get("lostOnSend"))
                        .build();

                batchPoints.point(point);

            } else {
                LOG.warn("Send-iconnection inner metric "
                        + e.getKey()
                        + " value was not a HashMap. It"
                        + " was a "
                        + e.getValue().getClass()
                        + ": " + e.getValue());
            }

            }

        }

    /**
     * A catch all method for all other metric types. It assumes the values of
     * supplied dataMap are numerical and adds them as "value" fields to a
     * measurement with the name "<metric>.<dataMap key>". If the value is not
     * numeric a entry is written to the log with details of the values type
     * and contents.
     *
     * @param metric The name of the metric being handled
     * @param timestamp The timestamp to be applied to this measurement
     * @param dataMap The map of data for this metric data point
     * @param batchPoints The current instance of BatchPoints that this metric
     *                    will be added to.
     */
    private void handle_other(String metric,
                              long timestamp,
                              Map dataMap,
                              BatchPoints batchPoints) {
        Map<String, Object> datamap =
                (Map<String, Object>) dataMap;

        for (Map.Entry<String, Object> e : datamap.entrySet()) {

            Number value = 0;
            try {
                value = (Number) e.getValue();
            } catch (ClassCastException exp) {
                LOG.warn("Value of sub-metric " + e.getKey()
                        + " of metric " + metric
                        + " was a " + e.getValue().getClass()
                        + ": " + e.getValue() + " not a number");
                break;
            }

            Point point = Point.measurement(
                    metric + "." + e.getKey())
                    .time(timestamp, TimeUnit.SECONDS)
                    .addField("value", value)
                    .build();

            //Add the point to this tasks batch
            batchPoints.point(point);

        }
    }
    }
