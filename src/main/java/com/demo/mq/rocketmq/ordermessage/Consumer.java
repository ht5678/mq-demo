package com.demo.mq.rocketmq.ordermessage;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 
 * 类Consumer.java的实现描述：顺序消费，带事务
 * @author yuezhihua 2015年10月22日 下午3:36:29
 */
public class Consumer {
    
    
    public static void main(String[] args) {
        try {
            
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_message_consumer");
            consumer.setNamesrvAddr("192.168.0.200:9876");
            /**
             * 设置consumer第一次启动时从队列头部开始消费，还是尾部开始消费
             * 如果非要第一次启动，那么按照上次消费的位置继续
             */
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        
            consumer.subscribe("TopicTestjjj", "TagA || TagC || TagD");
            
            consumer.registerMessageListener(new MessageListenerOrderly() {
                
                AtomicLong consumeTimes = new AtomicLong(0);
                
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    context.setAutoCommit(false);
                    System.out.println(Thread.currentThread().getName()+" Receive New Messages : "+msgs);
                    
                    //打印消息的详细信息
                    for(MessageExt ext : msgs){
                        System.out.println(ext.getTopic());
                        System.out.println(ext.getTags());
                        System.out.println(new String(ext.getBody()));
                    }
                    
                    this.consumeTimes.incrementAndGet();
                    
                    if((this.consumeTimes.get()%2 ==0)){
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                    else if(this.consumeTimes.get()%3==0){
                        return ConsumeOrderlyStatus.ROLLBACK;
                    }
                    else if(this.consumeTimes.get()%4==0){
                        return ConsumeOrderlyStatus.COMMIT;
                    }
                    else if(this.consumeTimes.get()%5==0){
                        context.setSuspendCurrentQueueTimeMillis(3000);
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                    
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            
            consumer.start();
        }
        catch (MQClientException e) {
            e.printStackTrace();
        }
        
    }

}
