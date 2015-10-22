package com.demo.mq.rocketmq.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;


/**
 * 
 * 类Producer.java的实现描述：我的第一次练习
 * @author yuezhihua 2015年10月22日 下午12:04:16
 */
public class Producer {
    
    
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("192.168.0.200:9876");
//        producer.setClientIP("192.168.0.200");
        producer.start();
        
        for(int i = 0 ; i < 10 ; i++){
            try{
                Message msg = new Message("TopicTest"   //topic
                    , "TagA"    //tag
                    ,("Hello RocketMQ"+i).getBytes()        //body
                    );
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
                
            }catch(Exception e){
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        
        producer.shutdown();
        
    }

}
