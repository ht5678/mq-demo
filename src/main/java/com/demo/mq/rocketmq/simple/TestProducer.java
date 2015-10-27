package com.demo.mq.rocketmq.simple;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 
 * 类TestProducer.java的实现描述：TODO 类实现描述 
 * @author yuezhihua 2015年10月27日 上午10:20:50
 */
public class TestProducer {

    
    public static void main(String[] args) throws MQClientException {
        
        /**
         * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br>
         * 注意：ProducerGroupName需要由应用来保证唯一<br>
         * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键，
         * 因为服务器会回查这个Group下的任意一个Producer
         */
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("192.168.0.200:9876");
        
        producer.start();
        
        for(int i = 0 ; i <1 ;i++){
            
            try {
                Message msg = new Message("TopicTest1", 
                    "TagA" , 
                    "key113", 
                    "Hello RocketMq".getBytes());
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
                
                QueryResult queryMessage = 
                        producer.queryMessage("TopicTest1", "key113", 10, 0, System.currentTimeMillis());
                
                for(MessageExt ext : queryMessage.getMessageList()){
                    System.out.println(ext);
                }
                
            }
            catch (RemotingException | MQBrokerException | InterruptedException e) {
                e.printStackTrace();
            }
            
        }
        
    }
    
}
