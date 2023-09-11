package io.github.alikelleci.kafkaserviceregistry.client;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaServiceRegistryClient {

  private final Producer<String, String> producer;
  private final Consumer<String, String> consumer;

  private String serviceId;
  private String requestTopic;
  private String replyTopic;

  private String clientId;
  private CompletableFuture<String> future;
  private Timer timer;


  protected KafkaServiceRegistryClient(Properties producerConfig, Properties consumerConfig, String serviceId, String requestTopic, String replyTopic) {
    this.serviceId = serviceId;
    this.requestTopic = requestTopic;
    this.replyTopic = replyTopic;

    this.clientId = serviceId + "@" + UUID.randomUUID().toString();
    this.future = new CompletableFuture<>();

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new StringSerializer());

    this.consumer = new KafkaConsumer<>(consumerConfig,
        new StringDeserializer(),
        new StringDeserializer());

    start();

    timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        if (future != null && future.isDone()) {
          ping();
        }
      }
    }, 5000, 5000);


    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      this.unregister();

      if (timer != null) {
        timer.cancel();
      }
      if (future != null) {
        future.cancel(true);
      }
    }));

  }

  public static KafkaServiceRegistryClientBuilder builder() {
    return new KafkaServiceRegistryClientBuilder();
  }

  @SneakyThrows
  public CompletableFuture<String> register() {
    ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic, serviceId, "register");
    record.headers().add("clientId", clientId.getBytes(StandardCharsets.UTF_8));

    log.info("Registering client (clientId={})", clientId);
    producer.send(record).get();

    return future;
  }

  @SneakyThrows
  public void unregister() {
    ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic, serviceId, "unregister");
    record.headers().add("clientId", clientId.getBytes(StandardCharsets.UTF_8));

    log.info("Unregistering client (clientId={})", clientId);
    producer.send(record).get();
  }

  @SneakyThrows
  private void ping() {
    ProducerRecord<String, String> record = new ProducerRecord<>(requestTopic, serviceId, "ping");
    record.headers().add("clientId", clientId.getBytes(StandardCharsets.UTF_8));

    log.debug("Pinging service registry (clientId={})", clientId);
    producer.send(record).get();
  }

  private void start() {
    AtomicBoolean closed = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      consumer.assign(Collections.singletonList(new TopicPartition(replyTopic, 0)));

      try {
        while (!closed.get()) {
          ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
          onMessage(consumerRecords);
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) throw e;
      } finally {
        consumer.close();
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      closed.set(true);
      consumer.wakeup();
    }));

    thread.start();
  }

  @SneakyThrows
  private void onMessage(ConsumerRecords<String, String> consumerRecords) {
    for (ConsumerRecord<String, String> record : consumerRecords) {
      String clientId = Optional.of(record.headers().lastHeader("clientId"))
          .map(header -> new String(header.value(), StandardCharsets.UTF_8))
          .orElse(null);

      if (!StringUtils.equals(this.clientId, clientId)) {
        continue;
      }

      if (future != null) {
        future.complete(record.value());
        log.info("Instance-id received (clientId={}, instanceId={})", clientId, record.value());
      }
    }
  }

  public static class KafkaServiceRegistryClientBuilder {

    private Properties producerConfig;
    private Properties consumerConfig;
    private String serviceId;
    private String requestTopic;
    private String replyTopic;

    public KafkaServiceRegistryClientBuilder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
      this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

      return this;
    }

    public KafkaServiceRegistryClientBuilder serviceId(String serviceId) {
      this.serviceId = serviceId;
      return this;
    }

    public KafkaServiceRegistryClientBuilder requestTopic(String requestTopic) {
      this.requestTopic = requestTopic;
      return this;
    }

    public KafkaServiceRegistryClientBuilder replyTopic(String replyTopic) {
      this.replyTopic = replyTopic;
      return this;
    }

    public KafkaServiceRegistryClient build() {
      this.consumerConfig = new Properties();
      String bootstrapServers = this.producerConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      if (StringUtils.isNotBlank(bootstrapServers)) {
        this.consumerConfig.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      }
      String securityProtocol = this.producerConfig.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
      if (StringUtils.isNotBlank(securityProtocol)) {
        this.consumerConfig.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      }
      this.consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      this.consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      this.consumerConfig.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
      this.consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      this.consumerConfig.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


      return new KafkaServiceRegistryClient(producerConfig, consumerConfig, serviceId, requestTopic, replyTopic);
    }
  }
}
