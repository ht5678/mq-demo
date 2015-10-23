package com.demo.mq.rocketmq.ordermessage;

import java.util.List;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 
 * 类Producer.java的实现描述：顺序写
 * @author yuezhihua 2015年10月22日 下午3:26:19
 */
public class Producer {

    
    public static void main(String[] args) {
        
        try {
            DefaultMQProducer producer = new DefaultMQProducer("order_message_producer");
            producer.setNamesrvAddr("192.168.0.200:9876");
            producer.start();
            
            String[] tags = new String[]{"TagA","TagB","TagC","TagD","TagE"};
            
            for(int i = 0 ; i < 100 ; i++){
                //订单id相同的消息要有序
                int orderId = i%10;
                Message msg = new Message("TopicTestjjj", tags[i%tags.length], "KEY"+i , ("Hello , RocketMQ"+i).getBytes());
                
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer)arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, orderId);
                System.out.println(sendResult);
            }
            
            producer.shutdown();
            
        }
        catch (MQClientException e) {
            e.printStackTrace();
        }
        catch (RemotingException e) {
            e.printStackTrace();
        }
        catch (MQBrokerException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        
        
    }
    
}
