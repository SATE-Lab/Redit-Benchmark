package io.redit.samples.kafka8623;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class Rest {

    private final SendService sendService;

    public Rest(SendService sendService) {
        this.sendService = sendService;
    }

    @PostMapping("/send")
    public Mono<String> send() {
        return sendService.send();
    }
}