package com.minsumo.datacategorizer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by massih on 4/18/16.
 */
public class DCbolt extends BaseBasicBolt{
    private static final Logger LOG = LoggerFactory.getLogger(DCbolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("@@@@@@@@@@@@@********************"+tuple.getString(0)+"@@@@@@@@@@@@@CHELSKI KOSOHERE********************");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
