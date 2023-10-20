package ru.antonovak.kafkaservice.sender;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;
import ru.antonovak.kafkaservice.model.User;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class KafkaSenderExample {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, User> userKafkaTemplate;

    public KafkaSenderExample(KafkaTemplate<String, String> kafkaTemplate,
                              KafkaTemplate<String, User> userKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.userKafkaTemplate = userKafkaTemplate;
    }

    void sendCustomMessage(String topicName, User user) {
        userKafkaTemplate.send(topicName, user);
    }

    void sendMessage(String topicName, String message) {
        kafkaTemplate.send(topicName, message);
    }

    void sendMessageWithCallback(String topicName, String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
    }

    @KafkaListener(topics = "reflectoring-others")
    @SendTo("reflectoring-1")
    String listenAndReply(String message) {
        log.info("ListenAndReply [{}]", message);
        return "This is a reply sent after receiving message";
    }
}
