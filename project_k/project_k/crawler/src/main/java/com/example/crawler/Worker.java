package com.example.crawler;

import com.rabbitmq.client.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Worker {
    private final String inputHost;
    private final String taskQueue;
    private final String resultHost;
    private final String resultQueue;

    public Worker(String inputHost, String taskQueue, String resultHost, String resultQueue) {
        this.inputHost = inputHost;
        this.taskQueue = taskQueue;
        this.resultHost = resultHost;
        this.resultQueue = resultQueue;
    }

    public void start() throws Exception {
        ConnectionFactory taskFactory = new ConnectionFactory();
        taskFactory.setHost(inputHost);
        Connection taskConn = taskFactory.newConnection();
        Channel taskChan = taskConn.createChannel();
        taskChan.queueDeclare(taskQueue, true, false, false, null);
        // Убедимся, что очередь для результатов существует
        ConnectionFactory resFactory = new ConnectionFactory();
        resFactory.setHost(resultHost);
        try (Connection resConn = resFactory.newConnection();
             Channel resChan = resConn.createChannel()) {
            resChan.queueDeclare(resultQueue, true, false, false, null);
        }
        System.out.println("[Worker] Waiting for jobs...");
        DeliverCallback handler = (tag, delivery) -> {
            String url = new String(delivery.getBody(), "UTF-8");
            System.out.println("[Worker] Processing: " + url);
            try {
                Map<String, Object> article = extractArticle(url);
                if (article != null) {
                    ConnectionFactory outFactory = new ConnectionFactory();
                    outFactory.setHost(resultHost);
                    try (Connection outConn = outFactory.newConnection();
                         Channel outChan = outConn.createChannel()) {
                        outChan.queueDeclare(resultQueue, true, false, false, null);
                        String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(article);
                        outChan.basicPublish("", resultQueue, null, json.getBytes());
                        System.out.println("[Worker] Sent to result_queue");
                    }
                }
            } catch (Exception e) {
                System.err.println("[Worker] Error: " + e.getMessage());
            }
            taskChan.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        taskChan.basicConsume(taskQueue, false, handler, tag -> {});
    }

    private Map<String, Object> extractArticle(String url) {
        try {
            Document doc = Jsoup.connect(url).get();
            String title = doc.selectFirst("h1").text();
            String published = doc.selectFirst("time").attr("datetime");
            String author = doc.selectFirst(".tm-user-info__username") != null ? doc.selectFirst(".tm-user-info__username").text() : "";
            String content = doc.select(".article-formatted-body").text();
            Map<String, Object> map = new HashMap<>();
            map.put("title", title);
            map.put("pubDate", published);
            map.put("author", author);
            map.put("text", content);
            map.put("link", url);
            return map;
        } catch (IOException e) {
            System.err.println("[Worker] Failed to process: " + url);
            return null;
        }
    }
}
