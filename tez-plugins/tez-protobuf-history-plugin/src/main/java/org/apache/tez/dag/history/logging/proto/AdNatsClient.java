package org.apache.tez.dag.history.logging.proto;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import io.nats.client.*;
import io.nats.client.api.*;

public class AdNatsClient {

    private final String[] natsServers;
    //close connection when done
    private final Connection connection;
    //close management instance when done
    private final JetStreamManagement management;

    /**
     *  example: "10.90.5.81:4222,10.90.5.81:4223,10.90.5.81:4224"
     **/
    public AdNatsClient(String[] natsServerList) throws Exception {
        for (String server : natsServerList) {
            if(null == server || server.isEmpty())
                throw new Exception("Servers value is empty");
        }
        this.natsServers = natsServerList;
        this.connection = this.connectToNATS();
        this.management = connection.jetStreamManagement();
    }

    public Connection getConnection() {
        return this.connection;
    }

    public Connection connectToNATS() {
        try {
            Options.Builder builder = new Options.Builder();
            builder = builder.connectionTimeout(Duration.ofSeconds(10));
            builder = builder.servers(this.natsServers);
            // might happen that when NATS is in CONNECTED state, the connection is not alive
            // keep reconnecting to avoid stale connection, -1 would mean infinite reconnects.
            builder = builder.maxReconnects(-1);
            return Nats.connect(builder.build());
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return null;
    }

    public boolean isNatsConnected() {
        if(null == connection)
            return false;
        return connection.getStatus() == Connection.Status.CONNECTED;
    }

    public Connection.Status natsStatus() {
        return connection.getStatus();
    }

    public boolean doesStreamExist(String streamName) throws Exception {
        try {
            if(null != management.getStreamInfo(streamName)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e){
            return false;
        }
    }

    /***
     * Whenever registering a stream, it should be made aware of subjects to associate with.
     *
     * A subject can have some particular type of event, and the stream can fetch and provide
     * events across subjects. We will use a subject to track and persist messages per hive
     * query for example. So the order of events in a hive query ad hook would be sequential
     * and naturally ordered as per our need.
     *
     * A subject can be generated as a UUID and also the publisher writing to this subject
     * with UUID as name, should also send this UUID value in a common subject name such as
     * SUBJECTS, this way the consumer can first read from SUBJECTS and then subscribe and
     * iterate over all SUBJECTS, in each iteration consumer will make sure to generate
     * ADEvents with sequence per hive query, ex: QUERY_SUBMITTED and QUERY_FINISHED.
     *
     * @param streamName
     * @param subjects
     * @return
     */

    public StreamInfo registerStream(String streamName,
                                     String[] subjects, Long maxMessagesLimit) throws Exception {
        StreamConfiguration conf = StreamConfiguration.builder()
                .name(streamName)
                .storageType(StorageType.File)
                .subjects(subjects)
                .retentionPolicy(RetentionPolicy.Limits)
                .maxMessages(maxMessagesLimit)
                .build();
        return management.addStream(conf);
    }

    public boolean deleteStream(JetStreamManagement management, String streamName) throws Exception {
        return management.deleteStream(streamName);
    }

    public void closeNATSConnection(Connection natsConnection) throws InterruptedException {
        if(null != natsConnection) {
            natsConnection.close();
        }
    }

    public void storeConsumerOffset(String durableConsumerOffsetSubject, Long offset) throws Exception {
        JetStream stream = connection.jetStream();
        stream.publish(durableConsumerOffsetSubject,offset.toString().getBytes("UTF-8"));
    }

    public void storeConsumerProbeTimestampOffset(String durableConsumerOffsetSubject, String probeTimestampOffset) throws Exception {
        JetStream stream = connection.jetStream();
        stream.publish(durableConsumerOffsetSubject,probeTimestampOffset.getBytes("UTF-8"));
    }

    public String getConsumerProbeTimestampOffset(String consumerName, String durableConsumerOffsetSubject) throws Exception {
        JetStream stream = connection.jetStream();
        PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                .durable(consumerName)
                .name(consumerName)
                .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(1L).build())
                .build();
        JetStreamSubscription subscription = stream.subscribe(durableConsumerOffsetSubject, pullSubscribeOptions);
        List<Message> offsetList =  subscription.fetch(1,5000);
        if(null != offsetList && offsetList.size() > 0) {
            Message offsetBytesMessage = offsetList.get(offsetList.size()-1);
            if(null == offsetBytesMessage) {
                return "0,0";
            } else {
                return new String(offsetBytesMessage.getData(),"UTF-8");
            }
        }
        return "0,0";
    }

    public Long getConsumerOffset(String consumerName, String durableConsumerOffsetSubject) throws Exception {
        JetStream stream = connection.jetStream();
        PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                .durable(consumerName)
                .name(consumerName)
                .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(1L).build())
                .build();
        JetStreamSubscription subscription = stream.subscribe(durableConsumerOffsetSubject, pullSubscribeOptions);
        List<Message> offsetList =  subscription.fetch(1,5000);
        if(null != offsetList && offsetList.size() > 0) {
            Message offsetBytesMessage = offsetList.get(offsetList.size()-1);
            if(null == offsetBytesMessage) {
                return 0L;
            } else {
                Long offset = Long.parseLong(new String(offsetBytesMessage.getData(),"UTF-8"));
                return offset;
            }
        }
        return 0L;
    }

    public JetStreamSubscription subscribeToEvents(String consumerName, String eventsSubject, Long consumerLastOffset) throws Exception {
        PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                .durable(consumerName)
                .name(consumerName)
                .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(consumerLastOffset).build())
                .build();
        return this.connection.jetStream().subscribe(eventsSubject, pullSubscribeOptions);
    }

    public List<Message> getEvents(JetStreamSubscription subscription, int batchSize) {
        List<Message> events = new ArrayList<>();
        subscription.fetch(batchSize,4000).forEach(
                message -> {
                    events.add(message);
                }
        );
        return events;
    }

    public boolean deleteConsumer(String streamName, String consumerName) throws IOException, JetStreamApiException {
        ConsumerInfo consumerInfo = null;
        try {
            consumerInfo = this.management.getConsumerInfo(streamName, consumerName);
        } catch (Exception e) {
        }

        if(null != consumerInfo) {
            return this.management.deleteConsumer(streamName, consumerName);
        } else {
            return true;
        }
    }
}