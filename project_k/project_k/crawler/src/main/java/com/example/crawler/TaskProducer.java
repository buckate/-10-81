package com.example.crawler;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TaskProducer {
    private final String queueName;
    private final ConnectionFactory factory;

    public TaskProducer(String host, String queueName) {
        this.queueName = queueName;
        this.factory = new ConnectionFactory();
        this.factory.setHost(host);
    }

    public void enqueueTask(String data) throws Exception {
        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {
            channel.queueDeclare(queueName, true, false, false, null);
            channel.basicPublish("", queueName, null, data.getBytes());
            System.out.println("[TaskProducer] Enqueued: " + data);
        }
    }

    public void sendTask(String msg) throws Exception {
        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {
            channel.queueDeclare(queueName, true, false, false, null);
            channel.basicPublish("", queueName, null, msg.getBytes());
            System.out.println("[TaskProducer] Sent: " + msg);
        }
    }
}
