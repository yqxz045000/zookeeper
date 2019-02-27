package com.cfyj.demo.zookeeper.demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import lombok.extern.slf4j.Slf4j;

/**
 * 加入组：
 * 	1.当父节点为临时节点时，发现创建子节点失败，抛出异常：list==null
 * 	2.重复创建相同的永久节点时抛出异常： list==null
 *  
 * @author B-0257
 *
 */
@Slf4j
public class Demo2JoinGroup extends Demo1CreateGroupAndAllMethod implements Watcher {
	
	public static void main(String[] args) {
		
		Demo2JoinGroup demo = new Demo2JoinGroup();
		
		demo.connection("10.0.12.152:2181", demo);
		demo.createGroup("/rootZnode","zootZnode",CreateMode.PERSISTENT);
		demo.createGroup("/rootZnode/childrenZnode","childrenZnode",CreateMode.EPHEMERAL);
		demo.outZnodes("/rootZnode");
		
		
	}
	
	
	
	
}
