package com.kafka.reactive.kafka.sec11;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"

        );
//        ReceiverOptions.create()
        var options = ReceiverOptions.<String,String>create(consumerConfig)
                .subscription(List.of("order-events"));
//        KafkaReceiver.create()
        KafkaReceiver.create(options)
                .receive()
                .groupBy(r -> Integer.parseInt(r.key())%5) // just for demo
                .flatMap(KafkaConsumer::batchProcess)
                .subscribe();


    }

    private static Mono<Void> batchProcess(GroupedFlux<Integer,ReceiverRecord<String,String>> flux){
        return flux
                .publishOn(Schedulers.boundedElastic())
                .doFirst(() -> log.info("flux is stating ----------------- mod: {}", flux.key()))
                .doOnNext(r -> log.info("Key:{}, value:{}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .then( Mono.delay(Duration.ofSeconds(1)))
                .then();
    }
}
