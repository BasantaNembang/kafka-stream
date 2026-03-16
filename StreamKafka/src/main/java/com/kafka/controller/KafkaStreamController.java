package com.kafka.controller;


import com.kafka.service.KafkaStreamService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stream")
@CrossOrigin("*")
public class KafkaStreamController {

    private final KafkaStreamService service;

    public KafkaStreamController(KafkaStreamService service) {
        this.service = service;
    }

    @PostMapping()
    public String performTransaction(){
        return service.performTransaction();
    }




}


