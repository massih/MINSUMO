package com.minsumo.datacategorizer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

import java.util.Map;

/**
 * Created by massih on 4/12/16.
 */
public class DCspout extends KafkaSpout {

    private SpoutOutputCollector spCollector;

    public DCspout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        spCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
