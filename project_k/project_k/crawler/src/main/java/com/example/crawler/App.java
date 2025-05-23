package com.example.crawler;

public class App {
    public static void main(String[] args) {
        System.out.println("=== News Aggregator Started ===");
        String rssUrl = "https://habr.com/ru/rss/articles/";
        String rabbitHost = "rabbitmq";
        String taskQ = "task_queue";
        String resultQ = "result_queue";
        String elasticHost = "elasticsearch";
        int elasticPort = 9200;
        String elasticIdx = "news";

        // Step 1: Fetch RSS and queue links
        RssCrawler feedGrabber = new RssCrawler();
        try {
            var newsBatch = feedGrabber.getNewsList(rssUrl);
            System.out.println("Fetched articles: " + newsBatch.size());
            TaskProducer linkProducer = new TaskProducer(rabbitHost, taskQ);
            for (var news : newsBatch) {
                linkProducer.sendTask(news.link);
            }
            // Ensure result queue exists
            TaskProducer resultProducer = new TaskProducer(rabbitHost, resultQ);
            resultProducer.sendTask("init");
        } catch (Exception e) {
            System.err.println("[App] RSS parse or queue error: " + e.getMessage());
        }

        // Step 2: Start worker for processing links
        try {
            Worker jobWorker = new Worker(rabbitHost, taskQ, rabbitHost, resultQ);
            new Thread(() -> {
                try {
                    jobWorker.start();
                } catch (Exception err) {
                    System.err.println("[App] Worker error: " + err.getMessage());
                }
            }).start();
        } catch (Exception e) {
            System.err.println("[App] Worker launch error: " + e.getMessage());
        }

        // Step 3: Start result consumer for ElasticSearch
        try {
            ResultConsumer elasticSink = new ResultConsumer(rabbitHost, resultQ);
            elasticSink.consume();
        } catch (Exception e) {
            System.err.println("[App] Result consumer error: " + e.getMessage());
        }
    }
}
