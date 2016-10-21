package uk.org.tomcooper.tracer;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Thomas Cooper
 * @version 0.2
 * @date 2016/09/19
 *
 * Forwards all metrics (of interest to the tracer modelling system) to an InfluxDB instance.
 */
public class TracerConsumer implements IMetricsConsumer {

    /** InfluxDB client instance for connection to the time series database */
    private transient InfluxDB influxDB;
    /** The name of the database in InfluxDB that metrics will be sent to */
    private transient String dbName;
    /** The unique name storm has given to the topology this consumer is monitoring */
    private transient String topologyID;

    /**
     * Method for preparing the Metric Consumer bolt. This sets up the InfluxDB connection client.
     *
     * @param stormConf Storm configuration map. This should contain the following keys: tracerdb.host (in the form http://hostname:port), tracerdb.name, tracerdb.user, tracerdb.password.
     * @param registrationArgument Optional arguments for the Metrics Bolt
     * @param context The topology context object.
     * @param errorReporter The error reporting object.
     */
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {

        String dbHost = (String) stormConf.get("tracerdb.host");
        dbName = (String) stormConf.get("tracerdb.name");
        String dbUser = (String) stormConf.get("tracerdb.user");
        String dbPwd = (String) stormConf.get("tracerdb.password");

        topologyID = (String) stormConf.get("storm.id");

        influxDB = InfluxDBFactory.connect(dbHost, dbUser, dbPwd);
    }

    /**
     * This is an implementation of the {@link org.apache.storm.metric.api.IMetricsConsumer#handleDataPoints(TaskInfo, Collection)} method.
     *
     * It will unpack and forward all relevant metrics to an influxDB database. It has logic for both storm standard metrics
     * and the custom metrics required by the tracer system (such as {@link uk.org.tomcooper.tracer.TransferTimeMetric}).
     *
     * @param taskInfo The taskInfo instance containing details of the task that produced this metric.
     * @param dataPoints The collection of metrics measurments provided by Storm.
     */
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {

        if(taskInfo.srcComponentId.equals("__system")){
            //Ignore the system bolt metrics for the time being
        } else {

            BatchPoints batchPoints = BatchPoints.database(dbName)
                    .tag("host", taskInfo.srcWorkerHost)
                    .tag("workerProcess", String.valueOf(taskInfo.srcWorkerPort))
                    .tag("taskID", String.valueOf(taskInfo.srcTaskId))
                    .tag("component", taskInfo.srcComponentId)
                    .tag("topology", topologyID)
                    .retentionPolicy("default")
                    .consistency(InfluxDB.ConsistencyLevel.ONE)
                    .build();

            long timestamp = taskInfo.timestamp;

            for (DataPoint p : dataPoints) {

                if(p.value instanceof Map){

                    Map dataMap = (Map) p.value;

                    //Some of the metrics maps can be empty so check they have values first
                    if(!dataMap.isEmpty()){

                        if(p.name.contains("count") | p.name.contains("latency")){
                            //The count and latency metrics contain keys for each stream
                            //add a separate point for each stream and value pair

                            Map<String, Object> datamap = (Map<String, Object>) dataMap;

                            for(Map.Entry<String, Object> e : datamap.entrySet()) {

                                Point point = Point.measurement(p.name.replaceAll("__", ""))
                                        .time(timestamp, TimeUnit.SECONDS)
                                        .tag("stream", e.getKey())
                                        .addField("value", (Number) e.getValue())
                                        .build();

                                //Add the point to this tasks batch
                                batchPoints.point(point);

                            }

                        } else if(p.name.equals("__sendqueue") | p.name.equals("__receive")){
                            //The sendqueue and receive metrics contain a list of measures for the queues of each component

                           Map<String, Object> datamap = (Map<String, Object>) dataMap;

                            for(Map.Entry<String, Object> e : datamap.entrySet()) {

                                Point point = Point.measurement(p.name.replaceAll("__", "") + "." + e.getKey())
                                        .time(timestamp, TimeUnit.SECONDS)
                                        .addField("value", (Number) e.getValue())
                                        .build();

                                //Add the point to this tasks batch
                                batchPoints.point(point);

                            }

                        } else if(p.name.contains("Transfer-")){

                            //The key for this metric is a string of the form Transfer-SourceComponent-StreamID-DestinationComponent
                            //The value (p.value) is a map from an integer taskID to a Double or Integer representing the average transfer latency or count
                            //Convert dataMap to the correct Map type.
                            Map<Integer, Object> datamap = (Map<Integer, Object>) dataMap;

                            //Loop over all the tasks in this metric and create a point of each
                            for(Map.Entry<Integer, Object> e : datamap.entrySet()) {

                                Point point = Point.measurement(p.name)
                                        .time(timestamp, TimeUnit.SECONDS)
                                        .tag("source-task", String.valueOf(e.getKey()))
                                        .addField("value", (Number) e.getValue())
                                        .build();

                                //Add the point to this tasks batch
                                batchPoints.point(point);
                            }

                        } else {
                            //This datapoint is for a metric we don't care about
                        }

                    } else {
                        //Empty Map value - Which is odd?
                    }

                } else if(p.value instanceof Number) {

                    //Some of the metrics are just numbers and so can just be entered as straight measurements
                    Point point = Point.measurement(p.name.replaceAll("__", ""))
                            .time(timestamp, TimeUnit.SECONDS)
                            .addField("value", (Number) p.value)
                            .build();

                    batchPoints.point(point);
                }

            }

            try {
                //Write the batch of points for this tasks metrics to InfluxDB.
                influxDB.write(batchPoints);
            } catch (NullPointerException n) {
                System.out.println("#######  Sending got a NullPointer Exception! ########");
                n.printStackTrace();
            } catch (Exception e) {
                System.out.println("#######  Sending got a another exception ########");
                e.printStackTrace();
            }

        }
    }

    public void cleanup() {
    }
}
