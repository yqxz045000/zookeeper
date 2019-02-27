package com.cfyj.demo.zookeeper.demo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import lombok.extern.slf4j.Slf4j;

/**
 * 创建znode
 * 	创建节点需要四个参数：
 * 	path:创建的节点名称 
 *  data:节点存储的数据
 *  acl:节点权限--》Ids
 *  createMode: 创建的节点类型--》CreateMode
 *  
 *  1.可以创建临时节点和顺序节点，并控制节点的权限
 * 	2.临时节点不能创建子节点，当回话到期或者客户端下线时，临时节点会删除,如果创建的节点其父节点是临时的，则抛出：KeeperException
 * 	3.创建顺序节点时，数字固定长度为10位，不够的0填充，每次创建时序号递增，创建的顺序节点名称为 path+递增序号，如： Demo1CreateGroup0000000007
 * 	4.如果创建的节点路径已经存在，则抛出这样的异常： KeeperException（非运行时异常）
 * 	5.如果创建的节点没有父节点则报出异常:KeeperException
 * 	6.如果此操作成功，将触发监控，如该父节点的监控，或当前路径的监控
 * 	7.数据存储最大允许1MB
 * 	
 * 
 * 创建节点的类型： CreateMode
 * 	PERSISTENT   永久节点，客户端断开也不会删除该节点
 * 	PERSISTENT_SEQUENTIAL   永久递增顺序节点
 * 	EPHEMERAL  临时节点，客户端断开时删除
 *  EPHEMERAL_SEQUENTIAL  临时递增顺序节点，
 *  
 *  acl: Ids
 *  
 *  OPEN_ACL_UNSAFE
 *  
 *  
 *  
 * @author B-0257
 *
 */
@Slf4j
public class Demo1CreateGroupAndAllMethod implements Watcher {

	private CountDownLatch connectedSignal = new CountDownLatch(1);

	private ZooKeeper zkClient = null;

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			log.info("连接创建完成,sessionInfo,sessionId:{},sessionPW:{},sessionPWD:{}",zkClient.getSessionId(),zkClient.getSessionPasswd(),zkClient.getSessionTimeout());
		
			connectedSignal.countDown();
		}

	}

	public static void main(String[] args) throws InterruptedException {

		Demo1CreateGroupAndAllMethod demo = new Demo1CreateGroupAndAllMethod();

		demo.connection("10.0.12.152:2181", demo);
		demo.createGroup("/Demo1CreateGroup","frist create znode");
		demo.outZnodes("/");
	}
	
	
	public void outZnodes(String path) {
		
		try {
			log.info("输出该节点下所有子节点");
			List<String> znodes = zkClient.getChildren(path, false);
			printList(znodes);
		} catch (KeeperException | InterruptedException e) {
			log.error("获取节点异常,path:{}",path,e);
		}
		
	}
	
	public static void printList(List<String> list) {
		if(list==null) return ;
		
		for(Object obj :list) {
			log.info(obj.toString());
			
		}
	}
	
	public void connection(String connectString, Watcher watcher) {

		try {
			zkClient = new ZooKeeper(connectString, 20000, watcher);
			connectedSignal.await(); //因为创建连接是异步的，创建完成后会立即返回，这里使用计时器，等待连接完成后再开始执行后面的操作
			log.info("锁释放------------");
		} catch (Exception e) {
			log.error("创建zkClient异常", e);
		}
	}

	public void createGroup(String path,String data) {
		
		createGroup(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	public void createGroup(String path,String data , CreateMode type) {
		if(type==null) {
			type = CreateMode.EPHEMERAL_SEQUENTIAL ; 
		}
		try {
			String result  = zkClient.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, type);//返回的是节点的名称
			log.info("创建节点完成,path:{},data:{},result:{}",path,data,result);
			
		} catch (KeeperException | InterruptedException e) {
			log.error("创建节点失败",e);
		}	
	}
	

	public CountDownLatch getConnectedSignal() {
		return connectedSignal;
	}

	public void setConnectedSignal(CountDownLatch connectedSignal) {
		this.connectedSignal = connectedSignal;
	}

	public ZooKeeper getZkClient() {
		return zkClient;
	}

	public void setZkClient(ZooKeeper zkClient) {
		this.zkClient = zkClient;
	}

}
