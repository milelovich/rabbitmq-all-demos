package com.flamexander.rabbitmq.console.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;


public class BlogConsumer {
    private static final String EXCHANGE_NAME = "topic_exchange";
    private static final String ROUTING = "itblog";

    public static void main(String[] args) throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = null;
        try {
            connection = factory.newConnection();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            System.out.println(Thread.currentThread().getName());
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });


        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String command = scanner.nextLine();
            String [] splitStr = command.trim().split("\\s+");

            switch (splitStr[0]){
                case "set_topic":
                    channel.queueBind(queueName,EXCHANGE_NAME, ROUTING + splitStr[1]);
                    System.out.println("[*] Waiting " + ROUTING + splitStr[1] );
                    break;

                case "unset_topic":
                    channel.queueBind(queueName,EXCHANGE_NAME, ROUTING + splitStr[1]);
                    System.out.println("[*] Stop waiting " + ROUTING + splitStr[1] );
                    break;

            }
        }
        scanner.close();

    }



}
