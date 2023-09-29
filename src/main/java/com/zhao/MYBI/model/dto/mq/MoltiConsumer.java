package com.zhao.MYBI.model.dto.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class MoltiConsumer {

  private static final String TASK_QUEUE_NAME = "mutil_queue";

  public static void main(String[] argv) throws Exception {
//      建立连接
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();
//创建通道
    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

//当该消费者在接收到队列里的消息但没有返回确认结果之前,它不会将新的消息分发到这里
//    channel.basicQos(1);

//      如何处理消息
    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");

        try {
//         处理工作逻辑
            System.out.println(" [x] Received '" + message + "'");

            Thread.sleep(20000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println(" [x] Done");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    };
//    开启消费监听
    channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
  }

//  private static void doWork(String task) {
//    for (char ch : task.toCharArray()) {
//        if (ch == '.') {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException _ignored) {
//                Thread.currentThread().interrupt();
//            }
//        }
//    }
//  }
}