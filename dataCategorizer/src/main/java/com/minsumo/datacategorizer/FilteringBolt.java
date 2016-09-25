package com.minsumo.datacategorizer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.xerces.parsers.DOMParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;


/**
 * Created by massih on 4/18/16.
 */
public class FilteringBolt extends BaseRichBolt{

    private DOMParser parser;
    private OutputCollector collector;

    private static final Logger LOG = LoggerFactory.getLogger(FilteringBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        parser = new DOMParser();
    }

    @Override
    public void execute(Tuple tuple) {
        String tupleString = tuple.getString(0);
        System.out.println(tupleString);
        try {
            parser.parse(new InputSource(new StringReader(tupleString)));
            Document xmlDoc = parser.getDocument();
            NodeList vehicle = xmlDoc.getElementsByTagName("vehicle");
            for (int i=0; i < vehicle.getLength();i++) {
                Element vehicleData = (Element) vehicle.item(i);
                /*
                System.out.println(
                        "id: " + vehicleData.getAttribute("id") +
                        " firstTimeStamp: " + vehicleData.getAttribute("firstTimestamp") +
                        " Speed: " + vehicleData.getAttribute("speed") +
                        " X: " + vehicleData.getAttribute("x") +
                        " Y: " + vehicleData.getAttribute("y"));
                */
                collector.emit(
                        new Values(
                        vehicleData.getAttribute("firstTimestamp"),
                        vehicleData.getAttribute("id"),
                        vehicleData.getAttribute("speed"),
                        vehicleData.getAttribute("x"),
                        vehicleData.getAttribute("y")
                        )
                );

                collector.ack(tuple);
            }
        } catch (SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timeStep","firstTimestamp","vehicleID","speed", "longitude","latitude"));
    }

}
