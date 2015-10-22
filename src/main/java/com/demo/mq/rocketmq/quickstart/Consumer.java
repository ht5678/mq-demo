package com.demo.mq.rocketmq.quickstart;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 
 * 类Consumer.java的实现描述：订阅消息 
 * @author yuezhihua 2015年10月22日 下午12:10:17
 */
public class Consumer {

    
    public static void main(String[] args) throws MQClientException {
    
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");
        consumer.setNamesrvAddr("192.168.0.200:9876");
        /**
         * 设置consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
         * 如果不是第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        
        consumer.subscribe("TopicTest", "*");
        
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName()+"Receive New Messages "+msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        
        
        consumer.start();
        
        System.out.println("Consumer Start");
        
    }
    
}
