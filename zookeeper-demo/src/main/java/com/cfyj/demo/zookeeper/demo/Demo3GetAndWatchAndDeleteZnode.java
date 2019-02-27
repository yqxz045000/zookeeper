package com.cfyj.demo.zookeeper.demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import lombok.extern.slf4j.Slf4j;

/**
 * 获取节点并设置观察,删除节点触发观察
 * 	getChildren(path, watcher):获取指定路径的子节点列表，并且设置观察对象，当该节点被删除，或者该节点的子节点创建或删除时触发观察时间
 * 	delete(path,version):path为指定删除的节点路径，version为版本号，如果所提供的版本号与znode的版本号一致，ZooKeeper会删除这个znode。这是一种乐观的加锁机制，使客户端能够检测出对znode的修改冲突。通过将版本号设置为-1，
 * 		可以绕过这个版本检测机制，不管znode的版本号是什么而直接将其删除。ZooKeeper不支持递归的删除操作，因此在删除父节点之前必须先删除子节点。
 * 		1.如删除的path不存在抛出：KeeperException
 * 		2.给定的版本不匹配抛出：KeeperException
 * 		3.如果给点path存在子节点则抛出： KeeperException
 * 		4.如果调用成功，则触发在此节点设置的观察事件
 * 
 * @author B-0257
 *
 */
@Slf4j
public class Demo3GetAndWatchAndDeleteZnode extends Demo1CreateGroupAndAllMethod implements Watcher {

	@Override
	public void process(WatchedEvent event) {
	
		super.process(event);
		log.info("触发,state:{}", event.getState());
		
	}
	
	
	public static void main(String[] args) {
		
		Demo3GetAndWatchAndDeleteZnode demo = new Demo3GetAndWatchAndDeleteZnode();
		
		demo.connection("10.0.12.152:2181", demo);
//		demo.createGroup("/rootZnode","zootZnode",CreateMode.PERSISTENT);
		demo.createGroup("/rootZnode/watchZnode","watchZnode",CreateMode.PERSISTENT);
		demo.getZnodeAndWatch("/rootZnode",demo);
		demo.deleteZnode("/rootZnode/watchZnode");
		
	}
	
	public void getZnodeAndWatch(String path,Watcher watcher) {
		try {
			log.info("设置子节点，并设置观察事件");
			super.getZkClient().getChildren(path, watcher);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public void deleteZnode(String path) {
		
		try {
			super.getZkClient().delete(path, -1);
		} catch (InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//递归删除节点
	
	
	
	
    private void close() throws InterruptedException {
    	super.getZkClient().close();
    }
	
}
