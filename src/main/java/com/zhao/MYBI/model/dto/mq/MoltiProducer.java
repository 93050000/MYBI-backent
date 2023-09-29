package com.zhao.MYBI.model.dto.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.util.Scanner;

public class MoltiProducer {

  private static final String TASK_QUEUE_NAME = "mutil_queue";

  public static void main(String[] argv) throws Exception {
//      建立连接
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
//        队列持久化
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

//        持久输入
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String message = scanner.nextLine();

//            消息持久化（MessageProperties.PERSISTENT_TEXT_PLAIN）
            channel.basicPublish("", TASK_QUEUE_NAME,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes("UTF-8"));


            System.out.println(" [x] Sent '" + message + "'");
        }
    }
  }

}