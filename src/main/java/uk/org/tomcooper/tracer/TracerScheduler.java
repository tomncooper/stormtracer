package uk.org.tomcooper.tracer;

import com.google.common.collect.Sets;

import org.apache.storm.scheduler.*;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author Thomas Cooper
 * @date 19/10/16
 * <p>
 * This scheduler implementation is based on the simple round robin Default Scheduler in Storm. However, when it creates
 * a new physical plan for the executors on the cluster it will first send this to the Tracer service which will model
 * weather this new plan will meet the performance requirements for each topology before it is deployed to the cluster.
 */
public class TracerScheduler implements IScheduler {

    /**
     * The address of the tracer host including port number
     */
    private String tracerHost;

    /**
     * Method for preparing the Tracer Scheduler. This sets up the connection to the tracer service.
     *
     * @param conf - Map containing the Storm Cluster configuration from the $STORM_HOME/conf/storm.yaml file of the Nimbus
     *             node. This should contain the tracer host address.
     */
    @Override
    public void prepare(Map conf) {
        tracerHost = (String) conf.get("tracerHost");
    }

    private static final Logger LOG = LoggerFactory.getLogger(EvenScheduler.class);

    private static List<WorkerSlot> sortSlots(List<WorkerSlot> availableSlots) {
        //For example, we have a three nodes(supervisor1, supervisor2, supervisor3) cluster:
        //slots before sort:
        //supervisor1:6700, supervisor1:6701,
        //supervisor2:6700, supervisor2:6701, supervisor2:6702,
        //supervisor3:6700, supervisor3:6703, supervisor3:6702, supervisor3:6701
        //slots after sort:
        //supervisor3:6700, supervisor2:6700, supervisor1:6700,
        //supervisor3:6701, supervisor2:6701, supervisor1:6701,
        //supervisor3:6702, supervisor2:6702,
        //supervisor3:6703

        if (availableSlots != null && availableSlots.size() > 0) {
            // group by node
            Map<String, List<WorkerSlot>> slotGroups = new TreeMap<String, List<WorkerSlot>>();
            for (WorkerSlot slot : availableSlots) {
                String node = slot.getNodeId();
                List<WorkerSlot> slots = null;
                if (slotGroups.containsKey(node)) {
                    slots = slotGroups.get(node);
                } else {
                    slots = new ArrayList<WorkerSlot>();
                    slotGroups.put(node, slots);
                }
                slots.add(slot);
            }

            // sort by port: from small to large
            for (List<WorkerSlot> slots : slotGroups.values()) {
                Collections.sort(slots, new Comparator<WorkerSlot>() {
                    @Override
                    public int compare(WorkerSlot o1, WorkerSlot o2) {
                        return o1.getPort() - o2.getPort();
                    }
                });
            }

            // sort by available slots size: from large to small
            List<List<WorkerSlot>> list = new ArrayList<List<WorkerSlot>>(slotGroups.values());
            Collections.sort(list, new Comparator<List<WorkerSlot>>() {
                @Override
                public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
                    return o2.size() - o1.size();
                }
            });

            return interleaveAll(list);
        }

        return null;
    }

    private static <T> List<T> interleaveAll(List<List<T>> nodeList) {
        if (nodeList != null && nodeList.size() > 0) {
            List<T> first = new ArrayList<T>();
            List<List<T>> rest = new ArrayList<List<T>>();
            for (List<T> node : nodeList) {
                if (node != null && node.size() > 0) {
                    first.add(node.get(0));
                    rest.add(node.subList(1, node.size()));
                }
            }
            List<T> interleaveRest = interleaveAll(rest);
            if (interleaveRest != null) {
                first.addAll(interleaveRest);
            }
            return first;
        }
        return null;
    }

    /**
     * "{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"
     * <p>
     * Example usage in java:
     * Map<Integer, String> tasks;
     * Map<String, List<Integer>> componentTasks = Utils.reverse_map(tasks);
     * <p>
     * The order of he resulting list values depends on the ordering properties
     * of the Map passed in. The caller is responsible for passing an ordered
     * map if they expect the result to be consistently ordered as well.
     *
     * @param map to reverse
     * @return a reversed map
     */
    public static <K, V> HashMap<V, List<K>> reverseMap(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Entry<K, V> entry : map.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            List<K> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<K>();
                rtn.put(entry.getValue(), list);
            }
            list.add(key);
        }
        return rtn;
    }


    private static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        return reverseMap(executorToSlot);
    }

    private static Map<ExecutorDetails, WorkerSlot> scheduleTopology(TopologyDetails topology, Cluster cluster) {
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        Set<ExecutorDetails> allExecutors = (Set<ExecutorDetails>) topology.getExecutors();
        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
        int totalSlotsToUse = Math.min(topology.getNumWorkers(), availableSlots.size() + aliveAssigned.size());

        List<WorkerSlot> sortedList = sortSlots(availableSlots);
        if (sortedList == null || sortedList.size() < (totalSlotsToUse - aliveAssigned.size())) {
            LOG.error("Available slots are not enough for topology: {}", topology.getName());
            return new HashMap<ExecutorDetails, WorkerSlot>();
        }

        List<WorkerSlot> reassignSlots = sortedList.subList(0, totalSlotsToUse - aliveAssigned.size());
        Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
        for (List<ExecutorDetails> list : aliveAssigned.values()) {
            aliveExecutors.addAll(list);
        }
        Set<ExecutorDetails> reassignExecutors = Sets.difference(allExecutors, aliveExecutors);

        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<ExecutorDetails, WorkerSlot>();
        if (reassignSlots.size() == 0) {
            return reassignment;
        }

        List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>(reassignExecutors);
        Collections.sort(executors, new Comparator<ExecutorDetails>() {
            @Override
            public int compare(ExecutorDetails o1, ExecutorDetails o2) {
                return o1.getStartTask() - o2.getStartTask();
            }
        });

        for (int i = 0; i < executors.size(); i++) {
            reassignment.put(executors.get(i), reassignSlots.get(i % reassignSlots.size()));
        }

        if (reassignment.size() != 0) {
            LOG.info("Available slots: {}", availableSlots.toString());
        }
        return reassignment;
    }

    public static void scheduleTopologiesEvenly(Topologies topologies, Cluster cluster) {
        List<TopologyDetails> needsSchedulingTopologies = cluster.needsSchedulingTopologies(topologies);
        for (TopologyDetails topology : needsSchedulingTopologies) {
            String topologyId = topology.getId();
            Map<ExecutorDetails, WorkerSlot> newAssignment = scheduleTopology(topology, cluster);
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = reverseMap(newAssignment);

            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executors = entry.getValue();
                cluster.assign(nodePort, topologyId, executors);
            }
        }
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        scheduleTopologiesEvenly(topologies, cluster);
    }
}
