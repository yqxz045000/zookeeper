package com.cfyj.demo.zookeeper.demo;

import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

import lombok.extern.slf4j.Slf4j;


/**
 * 
 * 创建zookeeper：
 * 	ZooKeeper client = new ZooKeeper(connectString, sessionTimeout, watcher); 
 * 	 参数： connectString：以逗号分隔的主机:端口对的列表的连接字符串，会话建立是异步的，如果连接被中断后，会选择剩余未选择的重新连接（随机），	
 * 		1.此类的方法是线程安全的，除非另有说明。
 * 		2.当和zkServer创建连接时，会分配一个回话id和密码，客户端定期向服务端发送心跳，保持回话有效
 * 		3.当会话无效时，必须重新建立一个客户端对象。
 * 		4.如果连接当前服务失败的话，会去连接注册列表中其他的服务节点
 * 		5.ZooKeeper API方法要么是同步的，要么是异步的。同步的方法阻塞，直到服务器响应。异步方法只是排队
 * 		6.连接是异步建立的:当一个ZooKeeper的实例被创建时，会启动一个线程连接到ZooKeeper服务。由于对构造函数的调用是立即返回的，
 * 			因此在使用新建的ZooKeeper对象之前一定要等待其与ZooKeeper服务之间的连接建立成功。
 * 		7.临时节点没有子节点
 * 	连接创建日志分析：
 * 		1.客户端向zkServer发送建立连接的请求
 * 		2.服务端初始化session会话信息，返回给客户端
 * 		3.客户端重新发送请求至zkServer，至此连接建立完成
 * 		4.触发process()监听事件，
 * 
 * @author B-0257
 *
 */
@Slf4j
public class SimpleWatch implements Watcher{
	
	private int process_num = 0 ; 
	
	private static ZooKeeper client = null ; 
	
	@Override
	public void process(WatchedEvent event)  {
		// 0、触发监控时间,事件类型：WatchedEvent state:SyncConnected type:None path:null,
		log.info(process_num++ +"、触发监控时间,事件类型：{}," , event.toString());
		List<String> list;
		try {
			list = client.getChildren("/", false);
			printList(list);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		createConnection() ; 
		
	}
	
	
	public static void createConnection() throws Exception {
		 
		client = new ZooKeeper("10.0.12.152:2181", 1000, new SimpleWatch()); 
		//连接是异步建立的:当一个ZooKeeper的实例被创建时，会启动一个线程连接到ZooKeeper服务。由于对构造函数的调用是立即返回的，因此在使用新建的ZooKeeper对象之前一定要等待其与ZooKeeper服务之间的连接建立成功。
		for(int i = 0 ; i<10 ; i++) {
			log.info(i+"、当前节点的状态,connection:{},alive:{}"+ client.getState().isConnected(), client.getState().isAlive());
		}

		while(States.CONNECTED != client.getState()) {
			Thread.currentThread().sleep(100);
		}
		
		log.info("连接已建立");
		
		List<String> list= client.getChildren("/", false);	
		printList(list);
	}
	
	
	public static void printList(List<String> list) {
		int i = 0 ;
		for(Object obj :list) {
			log.info(i+++obj.toString());
			
		}
	}
	
	
}
