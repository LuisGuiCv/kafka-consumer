package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.repository.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;
    @Autowired
    ObjectMapper objectMapper;
public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
    LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
    log.info("libraryEvent: {}",libraryEvent);

    switch (libraryEvent.getLibraryEventType()){
        case NEW:
            save(libraryEvent);
            break;
        case UPDATE:
            validate(libraryEvent);
            save(libraryEvent);
            break;
        default:
            log.info("Invalid Library Event Type");

    }

}

    private void validate(LibraryEvent libraryEvent) {
    if (libraryEvent.getLibraryEventId()==null){
        throw new IllegalArgumentException("Library Event Id is missing");
    }
        Optional<LibraryEvent> libraryEventOptional=libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent()) {
            throw new IllegalArgumentException("Not a valid library Event");
        }
            log.info("Validation is successful for the library event: {}",libraryEventOptional.get());

    }

    private void save(LibraryEvent libraryEvent) {
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    libraryEventsRepository.save(libraryEvent);
    log.info("Succesfully persisted the library Event {} ",libraryEvent);
    }

}
