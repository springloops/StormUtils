package me.springloops.storm.spout;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.topology.base.BaseRichSpout;

public abstract class ScheduleJobSpout extends BaseRichSpout {
	
	private Date _firstTime;
	private long _delay;
	private long _period;
	
	private Timer timer;
	
	public ScheduleJobSpout(Date firstTime, long period) {
		_firstTime = firstTime;
		_period = period;
	}
	
	public ScheduleJobSpout(long delay, long period) {
		_delay = delay;
		_period = period;
	}
	
	public void activate() {
		setupTimer();
	}

	public void deactivate() {
		cancelTimer();
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
	}

	/**
	 * Call from timer at specific time. 
	 * You have to implement method <b>scheduleNextTuple()</b>.
	 * scheduleNextTuple method have to call to emit() method.
	 * 
	 * timer에 의해 특정 시간에 호출된다.
	 * scheduleNextTuple() 에서 emit()을 호출하도록 구현한다.
	 * 
	 */
	public abstract void scheduleNextTuple();
	
	private void setupTimer() {
		
		if (timer == null)
			timer = new Timer();
		
		if (_firstTime != null)
			timer.scheduleAtFixedRate(new Worker(), _firstTime, _period);
		else 
			timer.scheduleAtFixedRate(new Worker(), _delay, _period);
		
	}
	
	private void cancelTimer() {
		timer.cancel();
		timer.purge();
	}
	
	private class Worker extends TimerTask {
		
		@Override
		public void run() {
			scheduleNextTuple();
		}
		
	}
}
