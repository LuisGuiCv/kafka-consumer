package com.learnkafka.repository;

import com.learnkafka.entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventsRepository extends JpaRepository<LibraryEvent,Integer> {
}
