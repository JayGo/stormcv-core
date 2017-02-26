package nl.tno.stormcv.topology.reference;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class referenceTopology {
	public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("rSpout", new rSpout(),4);
        builder.setBolt("simpleBolt1", new simpleBolt(),4).shuffleGrouping("rSpout");
        builder.setBolt("statefulBolt1", new statefulBolt(),4).fieldsGrouping("simpleBolt1",new Fields("id"));
        builder.setBolt("simpleBolt2", new simpleBolt(),4).shuffleGrouping("statefulBolt1");
        builder.setBolt("statefulBolt2", new statefulBolt(),4).fieldsGrouping("simpleBolt2",new Fields("id"));
        builder.setBolt("simpleBolt3", new simpleBolt(),4).shuffleGrouping("statefulBolt2");
        builder.setBolt("statefulBolt3", new statefulBolt(),4).fieldsGrouping("simpleBolt3",new Fields("id"));
        builder.setBolt("simpleBolt4", new simpleBolt(),4).shuffleGrouping("statefulBolt3");
        builder.setBolt("statefulBolt4", new statefulBolt(),4).fieldsGrouping("simpleBolt4",new Fields("id"));
        builder.setBolt("simpleBolt5", new simpleBolt(),4).shuffleGrouping("statefulBolt4");
        builder.setBolt("statefulBolt5", new statefulBolt(),4).fieldsGrouping("simpleBolt5",new Fields("id"));
        builder.setBolt("simpleBolt6", new simpleBolt(),4).shuffleGrouping("statefulBolt5");
        builder.setBolt("statefulBolt6", new statefulBolt(),4).fieldsGrouping("simpleBolt6",new Fields("id"));
        
        builder.setBolt("ackBolt", new ackBolt()).shuffleGrouping("statefulBolt6");
        
        Config conf = new Config();
        
        List<String> list = new LinkedList<String>();
        list.add("rSpout");
        list.add("simpleBolt1");
        list.add("statefulBolt1");
        list.add("simpleBolt2");
        list.add("statefulBolt2");
        list.add("simpleBolt3");
        list.add("statefulBolt3");
        list.add("simpleBolt4");
        list.add("statefulBolt4");
        list.add("simpleBolt5");
        list.add("statefulBolt5");
        list.add("simpleBolt6");
        list.add("statefulBolt6");
        list.add("ackBolt");
        conf.put("components", list);
        Map<String, List<String>> streamMap = new HashMap<String, List<String>>();
        List<String> list1 = new LinkedList<String>();
        list1.add("simpleBolt1");
        streamMap.put("rSpout",list1);
        List<String> list2= new LinkedList<String>();
        list2.add("statefulBolt1");
        streamMap.put("simpleBolt1",list2);
        List<String> list3= new LinkedList<String>();
        list3.add("simpleBolt2");
        streamMap.put("statefulBolt1",list3);
        List<String> list4= new LinkedList<String>();
        list4.add("statefulBolt2");
        streamMap.put("simpleBolt2",list4);
        List<String> list5= new LinkedList<String>();
        list5.add("simpleBolt3");
        streamMap.put("statefulBolt2",list5);
        List<String> list6= new LinkedList<String>();
        list6.add("statefulBolt3");
        streamMap.put("simpleBolt3",list6);
        List<String> list7= new LinkedList<String>();
        list7.add("simpleBolt4");
        streamMap.put("statefulBolt3",list7);
        List<String> list8= new LinkedList<String>();
        list8.add("statefulBolt4");
        streamMap.put("simpleBolt4",list8);
        List<String> list9= new LinkedList<String>();
        list9.add("simpleBolt5");
        streamMap.put("statefulBolt4",list9);
        List<String> list10= new LinkedList<String>();
        list10.add("statefulBolt5");
        streamMap.put("simpleBolt5",list10);
        List<String> list11= new LinkedList<String>();
        list11.add("simpleBolt6");
        streamMap.put("statefulBolt5",list11);
        List<String> list12= new LinkedList<String>();
        list12.add("statefulBolt6");
        streamMap.put("simpleBolt6",list12);
        List<String> list13= new LinkedList<String>();
        list13.add("ackBolt");
        streamMap.put("statefulBolt6",list13);
        conf.put("streams", streamMap);
        
        conf.setDebug(false);
        conf.setNumWorkers(24);
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("WordCount", conf, builder.createTopology());
            try {
                    StormSubmitter.submitTopologyWithProgressBar("reference",
                            conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                    e.printStackTrace();
            } catch (InvalidTopologyException e) {
                    e.printStackTrace();
            } catch (AuthorizationException e) {
                    e.printStackTrace();
            }
    }
}