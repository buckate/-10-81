package com.example.crawler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.Map;

public class ResultConsumer {
    private final String queueName;
    private final ConnectionFactory factory;
    private final String elasticHost = "elasticsearch";
    private final int elasticPort = 9200;
    private final String elasticIndex = "news";
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public ResultConsumer(String brokerHost, String queueName) {
        this.queueName = queueName;
        this.factory = new ConnectionFactory();
        this.factory.setHost(brokerHost);
    }

    public void consume() throws Exception {
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare(queueName, true, false, false, null);
        System.out.println("[ResultConsumer] Waiting for results...");
        ElasticStorage elastic = new ElasticStorage(elasticHost, elasticPort, elasticIndex);
        DeliverCallback handler = (tag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("[ResultConsumer] Received: " + message);
            try {
                Map<String, Object> doc = jsonMapper.readValue(message, Map.class);
                String docId = elastic.computeId((String) doc.get("title"), (String) doc.get("pubDate"));
                elastic.saveArticle(doc, docId);
            } catch (Exception e) {
                System.err.println("[ResultConsumer] Elastic save error: " + e.getMessage());
            }
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        channel.basicConsume(queueName, false, handler, tag -> {});
    }
}
