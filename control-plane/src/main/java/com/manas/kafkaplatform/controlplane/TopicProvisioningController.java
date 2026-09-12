package com.manas.kafkaplatform.controlplane;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * The single self-service entry point: {@code POST /topics}. One HTTP call in, and the topic
 * exists, its dashboard JSON exists, and its storage-cost report exists, all three as a side
 * effect of this one endpoint.
 */
@RestController
public class TopicProvisioningController {

    private final TopicProvisioningService service;

    public TopicProvisioningController(TopicProvisioningService service) {
        this.service = service;
    }

    @PostMapping("/topics")
    public TopicProvisionResponse provision(@RequestBody TopicProvisionRequest request) {
        return service.provisionTopic(request);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> onBadRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(TopicAlreadyExistsException.class)
    public ResponseEntity<Map<String, String>> onConflict(TopicAlreadyExistsException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }
}
