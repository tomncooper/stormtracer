package uk.org.tomcooper.tracer.rpc;

import clojure.lang.IFn;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by tom on 21/10/16.
 */
public class TracerConnection {

    private final ManagedChannel channel;
    private final PlanCheckerGrpc.PlanCheckerBlockingStub blockingStub;
    private final PlanCheckerGrpc.PlanCheckerFutureStub futureStub;
    private final Logger LOG;

    public TracerConnection(String host, int port, Logger Log){
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true), Log);
    }

    public TracerConnection(ManagedChannelBuilder<?> channelBuilder, Logger Log){
        channel = channelBuilder.build();
        blockingStub = PlanCheckerGrpc.newBlockingStub(channel);
        futureStub = PlanCheckerGrpc.newFutureStub(channel);
        LOG = Log;
        Log.debug("TracerConnection object created");
    }

    private PhysicalPlan convertToPhysicalPlanMessage(Map<String, Map<WorkerSlot, List<ExecutorDetails>>> topologyAssignments){

        PhysicalPlan.Builder planMessageBuilder = PhysicalPlan.newBuilder();

        for(String tid : topologyAssignments.keySet()){

            Map<WorkerSlot, List<ExecutorDetails>> topology = topologyAssignments.get(tid);

            Topology.Builder topologyMessageBuilder = Topology.newBuilder().setId(tid);

            for(WorkerSlot worker : topology.keySet()) {

                WorkerProcess.Builder wpBuilder = WorkerProcess.newBuilder().setNodeID(worker.getNodeId()).setPort(worker.getPort());

                for(ExecutorDetails ex : topology.get(worker)){
                    Executor executorMessage = Executor.newBuilder().setStart(ex.getStartTask()).setEnd(ex.getEndTask()).build();
                    wpBuilder.addExecutors(executorMessage);
                }

                topologyMessageBuilder.addWorkerProcesses(wpBuilder.build());
            }

            planMessageBuilder.putTopologies(tid, topologyMessageBuilder.build());
        }

        return planMessageBuilder.build();
    }

    public boolean verifyPhysicalPlan(Map<String, Map<WorkerSlot, List<ExecutorDetails>>> topologyAssignments){

        PhysicalPlan plan = convertToPhysicalPlanMessage(topologyAssignments);

        boolean approved = false;

        try {
            TracerReply reply = blockingStub.verifyPhysicalPlan(plan);
            approved = reply.getGo();
        } catch (StatusRuntimeException e){
            LOG.warn("Connection to Tracer RPC server failed:\n" + e.getStatus());
        }
        return approved;
    }
}
