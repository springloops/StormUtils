package me.springloops.storm.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

public class DistributedLockSpout extends BaseRichSpout {

	private String thisComponentId;
	private int thisTaskId;
	private List<String> zkServers;
	private Integer zkPort;
	private String lockPath;
	
	private SpoutOutputCollector collector;
	private CuratorFramework zkClient;
	private InterProcessMutex lock;
	

	public DistributedLockSpout(Integer zkPort, String ... zkServers) {
		
		this(null, zkPort, zkServers);
	}
	
	public DistributedLockSpout(String lockPath, Integer zkPort, String ... zkServers) {
		
		this.zkServers = new ArrayList<String>();
		
		for (String zkServer : zkServers) {
			this.zkServers.add(zkServer);
		}
		
		this.zkPort = zkPort;
		this.lockPath = lockPath;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		if (zkServers == null || zkServers.size() < 1) {
			throw new RuntimeException("zkServer address info not found");
		}
		
		this.collector = collector;
		
		zkClient = Utils.newCuratorStarted(conf, zkServers, zkPort);
		
		if (lockPath == null) {
			StringBuilder sb = new StringBuilder();
			String stormId = context.getStormId();
			thisComponentId = context.getThisComponentId();
			thisTaskId = context.getThisTaskId();
			sb.append("/").append(stormId).append("/").append("_lock_").append(thisComponentId);
			lockPath = sb.toString();
		}
		
		lock = new InterProcessMutex(zkClient, lockPath);
	}

	@Override
	public void nextTuple() {
		try {
			if (!lock.acquire(10, TimeUnit.SECONDS)) {
				System.out.println(thisTaskId + " : resource locked!");
				
			} else {
//					System.out.println(thisTaskId + " : nextTuple");
					collector.emit(new Values(thisTaskId + " Test"), thisTaskId);
					Utils.sleep(10);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void ack(Object msgId) {
		System.out.println("ack" + msgId);
		release();
	}
	
	@Override
	public void fail(Object msgId) {
		System.out.println("fail" + msgId);
		release();
	}
	
	@Override
	public void close() {
		release();
		zkClient.close();
	}

	private void release() {
		try {
			if (lock.isAcquiredInThisProcess()) lock.release();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("test"));
	}
}
