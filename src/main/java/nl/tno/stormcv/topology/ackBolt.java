package nl.tno.stormcv.topology;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ackBolt extends BaseBasicBolt {
	private static Logger logger = LoggerFactory.getLogger(ackBolt.class);

	class sThread extends Thread{
		List<Integer> list;
		List<Integer> listNum;
		sThread(List<Integer> list,List<Integer> listNum){
			this.list=list;
			this.listNum=listNum;
		}
		@Override
		public void run(){
			super.run();
			while(true){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				for(int i=0;i<list.size();i++){
					//System.out.println(" aveTime="+list.get(i)+"; count="+listNum.get(i));
					logger.info("I: "+i+"; aveTime="+list.get(i)+"; count="+listNum.get(i));
				}
			}
		}
	}
	List<Integer> list = new LinkedList<Integer>();
	List<Integer> listNum = new LinkedList<Integer>();
	@Override
	public void prepare(Map stormConf,TopologyContext context) {
		sThread s = new sThread(list,listNum);
		s.start();
	}
	@Override
	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		// TODO Auto-generated method stub
		int endStime=(int) (System.currentTimeMillis()/1000);
		int count=arg0.getInteger(0);
		int startTime =arg0.getInteger(1);
		if(list.size()<count){
			list.add(endStime-startTime);
			listNum.add(1);
		}else{
			int aveTime=list.get(count-1);
			int num=listNum.get(count-1);
			//list.set(count-1, (aveTime*num+endStime-startTime)/(num+1));
			list.set(count-1, endStime-startTime);
			listNum.set(count-1, num+1);
			//System.out.println("index:"+count+"; aveTime="+endStime+"; aveTime="+startTime);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
