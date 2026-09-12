package com.manas.kafkaplatform.controlplane;

public class TopicAlreadyExistsException extends RuntimeException {
    public TopicAlreadyExistsException(String topicName) {
        super("topic already exists: " + topicName);
    }
}
