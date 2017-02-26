package nl.tno.stormcv.topology.reference;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

class single{
	private static single s=null;
	int[] r;
	private single(){
		r= new int[72000];
	}
	public static single getIns(){
		if(s==null){
			s=new single();
		}
		return s;
	}
}
public class rSpout extends BaseRichSpout implements IRichSpout {
	
	static Integer count = 0;
	private int index=0;
	private SpoutOutputCollector collector;
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		int time=(int) (System.currentTimeMillis()/1000);
		single s = single.getIns();
		collector.emit(new Values(index,time,s.r));
		//int num = (int) (1000/(100*(1-0.2*(1-(index-1)/3))));
		try {
			Thread t=Thread.currentThread();
			Thread.sleep(50);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		collector=arg2;
		synchronized(count){
			count++;
			index=count;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("id","startTime","pixel"));
	}

}
