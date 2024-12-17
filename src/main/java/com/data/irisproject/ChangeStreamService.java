package com.data.irisproject;



import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class ChangeStreamService {
    private final MongoRepository mongoRepository;

    private final MongoClient mongoClient;
    private final KafkaProducerService kafkaProducerService;


    public ChangeStreamService(MongoClient mongoClient, KafkaProducerService kafkaProducerService,MongoRepository mongoRepository) {
        this.mongoClient = mongoClient;
        this.kafkaProducerService = kafkaProducerService;
        this.mongoRepository = mongoRepository;

    }
    @Scheduled(fixedRate = 5000)
    @Async
    public void migrateData() throws Exception {
        List<UserEntity> entities = mongoRepository.findAll();
        for (UserEntity entity : entities) {

            kafkaProducerService.sendMessage("mongo-to-mysql", entity);
        }
    }
//    @PostConstruct
//    public void watchChanges() {
//        new Thread(() -> {
//            MongoDatabase database = mongoClient.getDatabase("tms");
//            MongoCollection<Document> collection = database.getCollection("users");
//
//            // Start listening to the change stream
//            collection.watch().forEach((ChangeStreamDocument<Document> change) -> {
//                if ("insert".equals(change.getOperationType().getValue()) || "update".equals(change.getOperationType().getValue())) {
//                    Document newDocument = change.getFullDocument();
//                    try {
////                        String message = objectMapper.writeValueAsString(newDocument);
//                        kafkaProducerService.sendMessage("mongo-to-mysql", newDocument);
//                    } catch (Exception e) {
//                        e.printStackTrace(); // Add proper error handling here
//                    }
//                }
//            });
//        }).start();
//    }
}

