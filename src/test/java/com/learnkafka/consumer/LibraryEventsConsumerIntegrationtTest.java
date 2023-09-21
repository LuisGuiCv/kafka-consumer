package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.repository.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest
@EmbeddedKafka(topics ={"library-events"},partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationtTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;


    @BeforeEach
    void setup(){
        for(MessageListenerContainer messageListenerContainer: endpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }
    @AfterEach
    void tearDown(){
        libraryEventsRepository.deleteAll();
    }
    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch=new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);
        Mockito.verify(libraryEventsConsumerSpy,Mockito.times(1)).onMessage(Mockito.isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy,Mockito.times(1)).processLibraryEvent(Mockito.isA(ConsumerRecord.class));
        List<LibraryEvent> libraryEventList = libraryEventsRepository.findAll();
        assert libraryEventList.size()==1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId()!=null;
            assertEquals(456,libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":457,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent=objectMapper.readValue(json,LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook= Book.builder().bookId(457).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Luigui").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson=objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(updatedJson).get();
        CountDownLatch latch=new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);
        Mockito.verify(libraryEventsConsumerSpy,Mockito.times(1)).onMessage(Mockito.isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy,Mockito.times(1)).processLibraryEvent(Mockito.isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 2.x",persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void publishInvalidLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":457,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch=new CountDownLatch(1);
        latch.await(10, TimeUnit.SECONDS);
        Mockito.verify(libraryEventsConsumerSpy,Mockito.times(10)).onMessage(Mockito.isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsServiceSpy,Mockito.times(10)).processLibraryEvent(Mockito.isA(ConsumerRecord.class));
         }
}
