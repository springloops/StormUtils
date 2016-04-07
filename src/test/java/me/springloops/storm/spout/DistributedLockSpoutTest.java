package me.springloops.storm.spout;

import java.util.Map;

import me.springloops.storm.spout.DistributedLockSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;

public class DistributedLockSpoutTest {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		DistributedLockSpout spout = new DistributedLockSpout(2181, "localhost");
		TestBolt2 bolt = new TestBolt2();

		builder.setSpout("testSpout", spout, 4).setNumTasks(4);
		builder.setBolt("testBolt", bolt).shuffleGrouping("testSpout")
				.setNumTasks(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-topology", new Config(),
				builder.createTopology());
	}

}

class TestBolt2 implements IRichBolt {

	private OutputCollector collector;

	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
//		collector.fail(input);
		String data = input.getString(0);
		System.out.println(data);
		collector.ack(input);
	}

	public void cleanup() {
		// TODO Auto-generated method stub
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
