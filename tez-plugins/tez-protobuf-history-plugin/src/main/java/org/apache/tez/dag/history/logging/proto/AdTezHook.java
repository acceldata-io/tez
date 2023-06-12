package org.apache.tez.dag.history.logging.proto;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.logging.proto.ProtoHistoryLoggingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class AdTezHook extends ProtoHistoryLoggingService {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static AdNatsClient natsClient = null;
    private static Connection natsConnection = null;
    private static JetStream jetStream = null;
    private String clusterName = null;
    private String natsWorkQueue = "tez_queries_history_events";
    private String natsWorkQueueEndpoint = "tez_queries_history_events_endpoint";
    private Configuration conf;

    private boolean persistToHDFS;

    private static final PublishOptions publishOptions = new PublishOptions.Builder().streamTimeout(java.time.Duration.ofSeconds(5)).build();

    private static Connection initializeNatsClient() {
        if (null != natsConnection && natsConnection.getStatus() == Connection.Status.CONNECTED) {
            return natsConnection;
        } else {
            return natsClient.connectToNATS();
        }
    }

    private static Set<HistoryEventType> eventsOfInterest() {
        return new HashSet<HistoryEventType>(Arrays.asList(
                HistoryEventType.DAG_SUBMITTED,
                HistoryEventType.DAG_FINISHED,
                HistoryEventType.VERTEX_INITIALIZED,
                HistoryEventType.VERTEX_FINISHED
        ));
    }

    private static Set<HistoryEventType> tezEventsTypeToCapture;

    private static final Logger LOG = LoggerFactory.getLogger(AdTezHook.class);

    private static JetStream getJetStream() throws Exception {
        return natsConnection.jetStream();
    }

    public boolean registerNatsResources(String[] natsServers, String clusterName) {
        LOG.debug("Going to execute registerNatsResources");
        if (null == natsClient || !natsClient.isNatsConnected()) {
            try {
                natsClient = new AdNatsClient(natsServers);
                natsConnection = initializeNatsClient();
                jetStream = getJetStream();
            } catch (Exception e) {
                LOG.warn("Exception in initializing Nats Resources, failed with exception ", e);
                return false;
            }
        }

        String workQueue = natsWorkQueue + "_" + clusterName;
        String workQueueEndpoint = natsWorkQueueEndpoint + "_" + clusterName;

        LOG.debug("Looking for work queue " + workQueue + " and endpoint subject " + workQueueEndpoint
                + " availability in NATS");
        //initialize nats work queue, use same name for stream and the subject
        //keeping limit as 1 million
        try {
            if (!natsClient.doesStreamExist(workQueue)) {
                StreamInfo streamInfo = natsClient.registerStream(workQueue, new String[]{workQueue}, 1000000L);
                LOG.debug("JetStream Info " + streamInfo);
            }
        } catch (Exception e) {
            LOG.warn("Error in getting JetStream info for NATS ", e);
            return false;
        }
        try {
            if (!natsClient.doesStreamExist(workQueueEndpoint)) {
                StreamInfo streamInfo = natsClient.registerStream(workQueueEndpoint, new String[]{workQueueEndpoint}, 1L);
                LOG.debug("JetStream Info " + streamInfo);
            }
            tezEventsTypeToCapture = eventsOfInterest();

        } catch (Exception e) {
            LOG.warn("Error in getting JetStream info for NATS ", e);
            return false;
        }
        return true;
    }

    public void serviceInit(Configuration serviceConf) throws Exception {
        super.serviceInit(serviceConf);
        this.conf = serviceConf;
        this.persistToHDFS = serviceConf.getBoolean("ad.hdfs.sink", true);
        try {
            this.clusterName = serviceConf.get("ad.cluster");
            String natsServerListCSV = serviceConf.get("ad.events.streaming.servers");
            registerNatsResources(natsServerListCSV.split(","), clusterName);
        } catch (Exception e) {
            LOG.debug("Exception in initializing Nats Resources, failed with exception", e);
        }
    }

    public AdTezHook() {
        super();
    }

    private String prepareTaskConfigForVertex(String vertexName, DAGProtos.PlanTaskConfiguration taskPlan) throws Exception {
        Map<String, Object> taskPlanParams = new HashMap<>();
        taskPlanParams.put("vertex_name", vertexName);
        taskPlanParams.put("num_tasks", taskPlan.getNumTasks());
        taskPlanParams.put("task_module", taskPlan.getTaskModule());
        taskPlanParams.put("virtual_cores", taskPlan.getVirtualCores());
        taskPlanParams.put("memory_mb", taskPlan.getMemoryMb());
        taskPlanParams.put("java_opts", taskPlan.getJavaOpts());

        //might not be useful, introduce in later hook versions.
        /*taskPlan.getEnvironmentSettingList().forEach(
            element -> {
                taskPlanParams.put("EnvironmentSetting_" + element.getKey(), element.getValue());
            }
        );
        taskPlan.getLocalResourceList().forEach(
                element -> {
                    taskPlanParams.put("LocalResourceType_" + element.getName(), element.getType());
                    taskPlanParams.put("LocalResourceUri_" + element.getName(), element.getUri());
                    taskPlanParams.put("LocalResourcePattern_" + element.getName(), element.getPattern());
                    taskPlanParams.put("LocalResourceSize_" + element.getName(), element.getSize());
                    taskPlanParams.put("LocalResourceTimestamp_" + element.getName(), element.getTimeStamp());
                }
        );*/
        return mapper.writeValueAsString(taskPlanParams);
    }

    private String prepareDAGSubmittedInfo(DAGSubmittedEvent dagSubmittedEvent) throws Exception {
        Map<String, Object> dagSubmittedParams = new HashMap<>();
        DAGProtos.DAGPlan dagPlan = dagSubmittedEvent.getDAGPlan();
        dagSubmittedParams.put("dag_info", dagPlan.getDagInfo());
        dagSubmittedParams.put("caller_id", dagPlan.getCallerContext().getCallerId());
        dagPlan.getDagConf().getConfKeyValuesList().forEach(
                dagConf -> {
                    dagSubmittedParams.put(dagConf.getKey(), dagConf.getValue());
                }
        );
        List<String> taskPlanConfigurations = new ArrayList<>();
        dagPlan.getVertexList().forEach(
                vertexPlan -> {
                    String taskPlanConf = null;
                    try {
                        taskPlanConf = prepareTaskConfigForVertex(vertexPlan.getName(), vertexPlan.getTaskConfig());
                    } catch (Exception e) {
                        LOG.error("Exception in task configuration persistence ", e);
                    }
                    if (null != taskPlanConf) {
                        taskPlanConfigurations.add(taskPlanConf);
                    }
                }
        );
        dagSubmittedParams.put("vertices_plan_task_conf", mapper.writeValueAsString(taskPlanConfigurations));
        dagSubmittedParams.put("dag_id", dagSubmittedEvent.getDagID().toString());
        dagSubmittedParams.put("app_id", dagSubmittedEvent.getDagID().getApplicationId().toString());
        dagSubmittedParams.put("submit_time", dagSubmittedEvent.getSubmitTime());
        dagSubmittedParams.put("dag_name", dagSubmittedEvent.getDAGName());
        dagSubmittedParams.put("attempt_id", dagSubmittedEvent.getApplicationAttemptId().getAttemptId());
        dagSubmittedParams.put("dag_user", dagSubmittedEvent.getUser());
        dagSubmittedParams.put("queue_name", dagSubmittedEvent.getQueueName());
        return mapper.writeValueAsString(dagSubmittedParams);
    }

    private String prepareVertexInitializedInfo(VertexInitializedEvent vertexInitializedEvent) throws Exception {
        Map<String, Object> vertexInitParams = new HashMap<>();
        vertexInitParams.put("vertex_id", vertexInitializedEvent.getVertexID().toString());
        vertexInitParams.put("name", vertexInitializedEvent.getVertexName());
        vertexInitParams.put("processor_name", vertexInitializedEvent.getProcessorName());
        vertexInitParams.put("dag_id", vertexInitializedEvent.getVertexID().getDAGId().toString());
        vertexInitParams.put("app_id", vertexInitializedEvent.getVertexID().getDAGId().getApplicationId().toString());
        vertexInitParams.put("init_time", vertexInitializedEvent.getInitedTime());
        vertexInitParams.put("num_task", vertexInitializedEvent.getNumTasks());
        return mapper.writeValueAsString(vertexInitParams);
    }

    private String prepareVertexFinishedInfo(VertexFinishedEvent vertexFinishedEvent) throws Exception {
        Map<String, Object> vertexFinishedParams = new HashMap<>();
        vertexFinishedParams.put("vertex_id", vertexFinishedEvent.getVertexID().toString());
        vertexFinishedParams.put("name", vertexFinishedEvent.getVertexName());
        vertexFinishedParams.put("dag_id", vertexFinishedEvent.getVertexID().getDAGId().toString());
        vertexFinishedParams.put("app_id", vertexFinishedEvent.getVertexID().getDAGId().getApplicationId().toString());
        vertexFinishedParams.put("diagnostics", vertexFinishedEvent.getDiagnostics());
        vertexFinishedParams.put("num_task", vertexFinishedEvent.getNumTasks());
        vertexFinishedParams.put("start_time", vertexFinishedEvent.getStartTime());
        vertexFinishedParams.put("finish_time", vertexFinishedEvent.getFinishTime());
        vertexFinishedParams.put("task_stats", mapper.writeValueAsString(vertexFinishedEvent.getVertexTaskStats()));
        vertexFinishedParams.put("state", vertexFinishedEvent.getState());
        Map<String, Map<String, Object>> tezCounters = new HashMap<>();
        //counters come in group, for each group we will have key, value pairs of individual counters
        vertexFinishedEvent.getTezCounters().forEach(
                tezCounterGroup -> {
                    String counterGroupName = tezCounterGroup.getName();
                    counterGroupName = counterGroupName.replaceAll("\\.", "_");
                    LOG.debug("Counter Group name is as " + counterGroupName);
                    Map<String, Object> counterGroupMetrics = new HashMap<>();
                    tezCounterGroup.forEach(
                            counter -> {
                                counterGroupMetrics.put(counter.getName(), counter.getValue());
                            }
                    );
                    tezCounters.put(counterGroupName, counterGroupMetrics);
                }
        );
        vertexFinishedParams.put("tez_counters", mapper.writeValueAsString(tezCounters));
        Map<String, Object> vertexStats = new HashMap<>();
        VertexStats stats = vertexFinishedEvent.getVertexStats();
        vertexStats.put("avg_task_duration", stats.getAvgTaskDuration());
        vertexStats.put("first_task_start_time", stats.getFirstTaskStartTime());
        vertexStats.put("last_task_finish_time", stats.getLastTaskFinishTime());
        vertexStats.put("last_tasks_to_finish", stats.getLastTasksToFinish().stream().map(task -> task.toString()).collect(Collectors.toList()));
        vertexStats.put("first_tasks_to_start", stats.getFirstTasksToStart().stream().map(task -> task.toString()).collect(Collectors.toList()));
        vertexStats.put("longest_duration_tasks", stats.getLongestDurationTasks().stream().map(task -> task.toString()).collect(Collectors.toList()));
        vertexStats.put("max_task_duration", stats.getMaxTaskDuration());
        vertexStats.put("min_task_duration", stats.getMinTaskDuration());
        vertexStats.put("shortest_duration_tasks", stats.getShortestDurationTasks().stream().map(task -> task.toString()).collect(Collectors.toList()));
        vertexFinishedParams.put("vertex_stats", mapper.writeValueAsString(vertexStats));
        return mapper.writeValueAsString(vertexFinishedParams);
    }

    private String prepareDagFinishedInfo(DAGFinishedEvent dagFinishedEvent) throws Exception {
        Map<String, Object> dagFinishedParams = new HashMap<>();
        DAGProtos.DAGPlan dagPlan = dagFinishedEvent.getDAGPlan();
        dagFinishedParams.put("dag_info", dagPlan.getDagInfo());
        dagFinishedParams.put("caller_id", dagPlan.getCallerContext().getCallerId());
        dagPlan.getDagConf().getConfKeyValuesList().forEach(
                dagConf -> {
                    dagFinishedParams.put(dagConf.getKey(), dagConf.getValue());
                }
        );
        List<String> taskPlanConfigurations = new ArrayList<>();
        dagPlan.getVertexList().forEach(
                vertexPlan -> {
                    String taskPlanConf = null;
                    try {
                        taskPlanConf = prepareTaskConfigForVertex(vertexPlan.getName(), vertexPlan.getTaskConfig());
                    } catch (Exception e) {
                        LOG.error("Exception in task configuration persistence ", e);
                    }
                    if (null != taskPlanConf) {
                        taskPlanConfigurations.add(taskPlanConf);
                    }
                }
        );
        dagFinishedParams.put("vertices_plan_task_conf", mapper.writeValueAsString(taskPlanConfigurations));
        dagFinishedParams.put("dag_id", dagFinishedEvent.getDagID().toString());
        dagFinishedParams.put("app_id", dagFinishedEvent.getDagID().getApplicationId().toString());
        dagFinishedParams.put("dag_name", dagFinishedEvent.getDagName());
        dagFinishedParams.put("state", dagFinishedEvent.getState());
        dagFinishedParams.put("start_time", dagFinishedEvent.getStartTime());
        dagFinishedParams.put("finish_time", dagFinishedEvent.getFinishTime());
        dagFinishedParams.put("attempt_id", dagFinishedEvent.getApplicationAttemptId().getAttemptId());
        dagFinishedParams.put("diagnostics", dagFinishedEvent.getDiagnostics());
        dagFinishedParams.put("dag_user", dagFinishedEvent.getUser());
        dagFinishedParams.put("dag_task_stats", mapper.writeValueAsString(dagFinishedEvent.getDagTaskStats()));
        //counters come in group, for each group we will have key, value pairs of individual counters
        Map<String, Map<String, Object>> tezCounters = new HashMap<>();
        dagFinishedEvent.getTezCounters().forEach(
                tezCounterGroup -> {
                    String counterGroupName = tezCounterGroup.getName();
                    counterGroupName = counterGroupName.replaceAll("\\.", "_");
                    LOG.debug("Counter Group name is as " + counterGroupName);
                    Map<String, Object> counterGroupMetrics = new HashMap<>();
                    tezCounterGroup.forEach(
                            counter -> {
                                counterGroupMetrics.put(counter.getName(), counter.getValue());
                            }
                    );
                    tezCounters.put(counterGroupName, counterGroupMetrics);
                }
        );
        dagFinishedParams.put("counters", mapper.writeValueAsString(tezCounters));
        dagFinishedParams.put("user", dagFinishedEvent.getUser());
        return mapper.writeValueAsString(dagFinishedParams);
    }

    @Override
    public void handle(org.apache.tez.dag.history.DAGHistoryEvent event) {

        long timePoint = System.currentTimeMillis();
        if (this.persistToHDFS) {
            super.handle(event);
            LOG.info("HDFS cost: " + (System.currentTimeMillis() - timePoint));
        }
        timePoint = System.currentTimeMillis();
        if (null == natsClient || !natsClient.isNatsConnected()) {
            LOG.debug("Going to miss this app for Pulse, NATS is down");
            return;
        }

        HistoryEventType eventType = event.getHistoryEvent().getEventType();

        Map<String, String> payload = null;

        if (tezEventsTypeToCapture.contains(eventType)) {
            LOG.debug("Event type is of Acceldata interest, value being: " + eventType);
            try {
                if (eventType == HistoryEventType.DAG_SUBMITTED) {
                    DAGSubmittedEvent dagSubmittedEvent = (DAGSubmittedEvent) event.getHistoryEvent();
                    payload = new HashMap<>();
                    payload.put("event_type", "DAG_SUBMITTED");
                    payload.put("event", prepareDAGSubmittedInfo(dagSubmittedEvent));
                } else if (eventType == HistoryEventType.VERTEX_INITIALIZED) {
                    VertexInitializedEvent vertexInitializedEvent = (VertexInitializedEvent) event.getHistoryEvent();
                    payload = new HashMap<>();
                    payload.put("event_type", "VERTEX_INITIALIZED");
                    payload.put("event", prepareVertexInitializedInfo(vertexInitializedEvent));
                } else if (eventType == HistoryEventType.VERTEX_FINISHED) {
                    VertexFinishedEvent vertexFinishedEvent = (VertexFinishedEvent) event.getHistoryEvent();
                    payload = new HashMap<>();
                    payload.put("event_type", "VERTEX_FINISHED");
                    payload.put("event", prepareVertexFinishedInfo(vertexFinishedEvent));
                } else if (eventType == HistoryEventType.DAG_FINISHED) {
                    DAGFinishedEvent dagFinishedEvent = (DAGFinishedEvent) event.getHistoryEvent();
                    payload = new HashMap<>();
                    payload.put("event_type", "DAG_FINISHED");
                    payload.put("event", prepareDagFinishedInfo(dagFinishedEvent));
                }

                if (null != payload) {
                    String natsPayload = mapper.writeValueAsString(payload);
                    String workQueue = natsWorkQueue + "_" + clusterName;
                    String workQueueEndpoint = natsWorkQueueEndpoint + "_" + clusterName;
                    PublishAck ack = jetStream.publish(workQueue, natsPayload.getBytes("UTF-8"), publishOptions);
                    jetStream.publish(workQueueEndpoint,
                            new Long(ack.getSeqno()).toString().getBytes("UTF-8"), publishOptions);
                }
                if (eventType == HistoryEventType.DAG_FINISHED) {
                    natsClient.closeNATSConnection(natsConnection);
                }
                payload = null;
                LOG.info("Streaming cost: " + (System.currentTimeMillis() - timePoint));
            } catch (Exception e) {
                LOG.error("Exception inside handle method, could not write to Acceldata NATS ", e);
            }
        }
    }
}
