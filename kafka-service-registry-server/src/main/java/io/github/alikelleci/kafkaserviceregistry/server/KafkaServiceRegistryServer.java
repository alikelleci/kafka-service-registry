package io.github.alikelleci.kafkaserviceregistry.server;

import io.github.alikelleci.kafkaserviceregistry.server.transformers.MessageTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaServiceRegistryServer {

  private Properties streamsConfig;
  private String requestTopic;
  private String replyTopic;

  private KafkaStreams kafkaStreams;

  protected KafkaServiceRegistryServer(Properties streamsConfig, String requestTopic, String replyTopic) {
    this.streamsConfig = streamsConfig;
    this.requestTopic = requestTopic;
    this.replyTopic = replyTopic;
  }

  public static KafkaServiceRegistryServerBuilder builder() {
    return new KafkaServiceRegistryServerBuilder();
  }

  private Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("clients"), Serdes.String(), Serdes.Integer())
        .withLoggingEnabled(Collections.emptyMap()));

    builder.stream(requestTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .transformValues(MessageTransformer::new, "clients")
        .filter((key, value) -> value != null)
        .to(replyTopic, Produced
            .with(Serdes.String(), Serdes.String())
            .withStreamPartitioner((topic, key, value, numPartitions) -> 0));

    return builder.build();
  }

  public void start() {
    if (kafkaStreams != null) {
      log.info("Server already started.");
      return;
    }

    this.kafkaStreams = new KafkaStreams(topology(), streamsConfig);
    setUpListeners();

    log.info("Server is starting...");
    kafkaStreams.start();
  }


  public void stop() {
    if (kafkaStreams == null) {
      log.info("Server already stopped.");
      return;
    }

    log.info("Server is shutting down...");
    kafkaStreams.close(Duration.ofMillis(5000));
    kafkaStreams = null;
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener((newState, oldState) ->
        log.warn("State changed from {} to {}", oldState, newState));

    kafkaStreams.setUncaughtExceptionHandler((throwable) ->
        StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);

    kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        log.debug("State restoration started: topic={}, partition={}, store={}, endingOffset={}", topicPartition.topic(), topicPartition.partition(), storeName, endingOffset);
      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        log.debug("State restoration in progress: topic={}, partition={}, store={}, numRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, numRestored);
      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        log.debug("State restoration ended: topic={}, partition={}, store={}, totalRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Server is shutting down...");
      kafkaStreams.close(Duration.ofMillis(5000));
    }));
  }

  public static class KafkaServiceRegistryServerBuilder {

    private Properties streamsConfig;
    private String requestTopic;
    private String replyTopic;

    public KafkaServiceRegistryServerBuilder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

      return this;
    }

    public KafkaServiceRegistryServerBuilder requestTopic(String requestTopic) {
      this.requestTopic = requestTopic;
      return this;
    }

    public KafkaServiceRegistryServerBuilder replyTopic(String replyTopic) {
      this.replyTopic = replyTopic;
      return this;
    }

    public KafkaServiceRegistryServer build() {
      return new KafkaServiceRegistryServer(this.streamsConfig, requestTopic, replyTopic);
    }
  }

}
