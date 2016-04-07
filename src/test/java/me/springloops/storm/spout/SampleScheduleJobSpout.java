package me.springloops.storm.spout;

import java.util.Date;
import java.util.Map;

import me.springloops.storm.spout.ScheduleJobSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SampleScheduleJobSpout extends ScheduleJobSpout {

	private SpoutOutputCollector collector;

	public SampleScheduleJobSpout(Date firstTime, long period) {
		super(firstTime, period);
	}

	public SampleScheduleJobSpout(long delay, long period) {
		super(delay, period);
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void scheduleNextTuple() {
		collector.emit(new Values("Test"), "test");
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("ack");
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("fail");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("scheduleSpout"));
	}

}
