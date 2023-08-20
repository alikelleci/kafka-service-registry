package io.github.alikelleci.kafkaserviceregistry.server.transformers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@Slf4j
public class MessageTransformer implements ValueTransformerWithKey<String, String, String> {

  private ProcessorContext context;
  private TimestampedKeyValueStore<String, Integer> clients;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.clients = processorContext.getStateStore("clients");

    long maxAge = 10_000;
    context.schedule(Duration.ofMillis(10_000), PunctuationType.WALL_CLOCK_TIME, currentTimestamp -> {

      AtomicLong counter = new AtomicLong(0);
      try (KeyValueIterator<String, ValueAndTimestamp<Integer>> iterator = clients.all()) {
        while (iterator.hasNext()) {
          KeyValue<String, ValueAndTimestamp<Integer>> keyValue = iterator.next();
          String clientId = keyValue.key;
          Integer number = keyValue.value.value();
          long timestamp = keyValue.value.timestamp();

          counter.incrementAndGet();

          if (currentTimestamp - timestamp > maxAge) {
            String instanceId = "INSTANCE-" + number;

            log.info("Unregistering client due expiration (clientId={}, instanceId={})", clientId, instanceId);
            clients.delete(clientId);

            counter.decrementAndGet();
          }
        }
      }
      log.debug("Number of clients running: {}", counter.get());
    });

  }

  @Override
  public String transform(String serviceId, String message) {
    String clientId = Optional.of(context.headers().lastHeader("clientId"))
        .map(header -> new String(header.value(), StandardCharsets.UTF_8))
        .orElse(null);

    if (StringUtils.isAnyBlank(serviceId, message, clientId)) {
      return null;
    }

    if (message.equals("register")) {
      ValueAndTimestamp<Integer> existing = clients.get(clientId);
      if (existing != null) {
        String instanceId = "INSTANCE-" + existing.value();

        log.info("Client already registered (clientId={}, instanceId={})", clientId, instanceId);
        return instanceId;
      }

      String from = serviceId.concat("@");
      String to = serviceId.concat("@z");

      List<Integer> ids = new ArrayList<>();
      try (KeyValueIterator<String, ValueAndTimestamp<Integer>> iterator = clients.range(from, to)) {
        while (iterator.hasNext()) {
          ids.add(iterator.next().value.value());
        }
      }

      int number = getNextAvailable(ids);
      String instanceId = "INSTANCE-" + number;

      log.info("Registering client (clientId={}, instanceId={})", clientId, instanceId);
      clients.put(clientId, ValueAndTimestamp.make(number, context.timestamp()));

      return instanceId;
    }

    if (message.equals("unregister")) {
      ValueAndTimestamp<Integer> existing = clients.get(clientId);
      if (existing != null) {
        String instanceId = "INSTANCE-" + existing.value();

        log.info("Unregistering client (clientId={}, instanceId={})", clientId, instanceId);
        clients.delete(clientId);
      }

    }

    if (message.equals("ping")) {
      ValueAndTimestamp<Integer> existing = clients.get(clientId);
      if (existing != null) {
        clients.put(clientId, ValueAndTimestamp.make(existing.value(), context.timestamp()));
      }
    }

    return null;
  }

  @Override
  public void close() {

  }

  private int getNextAvailable(List<Integer> ids) {
    return IntStream.iterate(0, n -> n + 1)
        .filter(n -> !CollectionUtils.emptyIfNull(ids).contains(n))
        .findFirst()
        .getAsInt();
  }

}
