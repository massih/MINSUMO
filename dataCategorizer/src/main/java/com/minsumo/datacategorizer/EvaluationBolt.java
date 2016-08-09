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
    private long startTime;
    private long endTime;
    private int globalCounter;
    private int counter;
    private boolean latency = false;
    private final int AVERAGE_COUNTER = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(EvaluationBolt.class);

    public EvaluationBolt() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        counter = 0;
        globalCounter = 0;
        averageLatency = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        if(latency){
            calculateLatency(tuple);
        }else{
            calculateTroughput(tuple);
        }
    }

    private void calculateLatency(Tuple tuple){
        if(globalCounter >= 5000){
            if (counter <= AVERAGE_COUNTER){
                Long ltc = (tuple.getLongByField("secondTimestamp") - tuple.getLongByField("firstTimestamp"));
                averageLatency += ltc;

                if(counter == AVERAGE_COUNTER){
                    averageLatency = averageLatency/AVERAGE_COUNTER;
                    System.out.println("%%%%%%%%%%%%%%%%The average latency for 1000 tuple is: "+ averageLatency + " %%%%%%%%%%%%%%%%");
                    counter = 0;
                    averageLatency = -1;
                }
                counter++;
            }
        }else{
            globalCounter++;
        }
    }

    private void calculateTroughput(Tuple tuple){
        if (globalCounter == 2000){
            startTime = System.currentTimeMillis();
            endTime = startTime + 1000;
            System.out.println(" START TIME = " + startTime);
            System.out.println(" END TIME = " + endTime);
            globalCounter++;
        }else if(globalCounter > 2000) {
            if(tuple.getLongByField("firstTimestamp") >= startTime && tuple.getLongByField("secondTimestamp") <= endTime){
                counter ++;
                System.out.println(counter + " tuples processed in 1 second");
            }
        }else if(globalCounter < 2000){
            globalCounter++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
