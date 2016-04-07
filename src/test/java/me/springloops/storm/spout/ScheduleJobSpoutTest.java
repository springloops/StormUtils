package me.springloops.storm.spout;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import me.springloops.storm.spout.ScheduleJobSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;

public class ScheduleJobSpoutTest {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		Calendar date = Calendar.getInstance();
		date.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY);
		date.set(Calendar.AM_PM, Calendar.PM);
		date.set(Calendar.HOUR, 3);
		date.set(Calendar.MINUTE, 28);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);
		
		long period = 1000 * 60;
		
		ScheduleJobSpout spout = new SampleScheduleJobSpout(date.getTime(), period);
		TestBolt bolt = new TestBolt();

		builder.setSpout("testSpout", spout, 1).setNumTasks(1);
		builder.setBolt("testBolt", bolt).shuffleGrouping("testSpout")
				.setNumTasks(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-topology", new Config(),
				builder.createTopology());
	}

}

class TestBolt implements IRichBolt {

	private OutputCollector collector;

	private List<String> result = new ArrayList<String>();
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
//		collector.fail(input);
		String data = input.getString(0);
		result.add(data);
		System.out.println("bolt list size : " + result.size());
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
