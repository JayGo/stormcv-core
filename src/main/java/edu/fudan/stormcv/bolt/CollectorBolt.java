package edu.fudan.stormcv.bolt;

import edu.fudan.stormcv.model.CVParticle;
import edu.fudan.stormcv.model.Frame;
import edu.fudan.stormcv.model.serializer.FrameSerializer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CollectorBolt extends CVParticleBolt {

    private static final long serialVersionUID = 4059835419496757476L;
    private FrameSerializer serializer = new FrameSerializer();

    @Override
    public void declareOutputFields(
            OutputFieldsDeclarer declarer) {
        declarer.declare(serializer.getFields());
    }

    @Override
    List<? extends CVParticle> execute(CVParticle input) throws Exception {
        List<Frame> result = new ArrayList<Frame>();
        Frame sf = (Frame) input;
//		while(!ReceiverQueue.getInstance().push(sf)) {
//			;
//		}
//		System.out.println("collect frame:" + sf.getSequenceNr());

        result.add(sf);

        for (CVParticle s : result) {
            for (String key : input.getMetadata().keySet()) {
                if (!s.getMetadata().containsKey(key)) {
                    s.getMetadata().put(key, input.getMetadata().get(key));
                }
            }
        }
        return result;
    }

    @Override
    void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub
//		SimpleOutput mSimpleOutput = new SimpleOutput();
//		new Thread(mSimpleOutput).start();
    }


}