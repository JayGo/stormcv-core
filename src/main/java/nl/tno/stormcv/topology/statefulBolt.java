package nl.tno.stormcv.topology;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class statefulBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		// TODO Auto-generated method stub
		int count=arg0.getInteger(0);
		int time =arg0.getInteger(1);
		int i=0;
		int [] r=(int [])arg0.getValue(2);
		//while(i<100000000){
		//	i++;
		//}
		arg1.emit(new Values(count,time,r));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("id","startTime","pixel"));
	}

}
