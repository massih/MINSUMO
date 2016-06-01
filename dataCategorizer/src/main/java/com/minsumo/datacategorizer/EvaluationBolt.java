package com.minsumo.datacategorizer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by massih on 5/31/16.
 */
public class EvaluationBolt extends BaseRichBolt{

    private OutputCollector collector;
    private long averageLatency;
    private final int MAXCOUNT = 100;
    private int counter;

    private static final Logger LOG = LoggerFactory.getLogger(EvaluationBolt.class);

    public EvaluationBolt() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        counter = 0;
        averageLatency = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        //System.out.println("%%%%%%%%%%%%%%%%number of fields: " + tuple.size());
        if (counter <= 1100){
            if (counter > 1000){
                Long ltc = (tuple.getLongByField("secondTimestamp") - tuple.getLongByField("firstTimestamp"));
                System.out.println("record number "+counter+ " latency is : " + ltc);
                averageLatency += ltc;
            }
            if(counter == 1100){
                averageLatency = averageLatency/100;
                System.out.println("%%%%%%%%%%%%%%%%The average latency for 100 tuple is: "+averageLatency);
            }
            counter++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
