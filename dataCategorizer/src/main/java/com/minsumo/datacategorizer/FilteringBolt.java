package com.minsumo.datacategorizer;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
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
public class FilteringBolt extends BaseBasicBolt{

    private DOMParser parser;

    private static final Logger LOG = LoggerFactory.getLogger(FilteringBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        parser = new DOMParser();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String tupleString = "<root>" + tuple.getString(0) + "</root>";
        try {
            parser.parse(new InputSource(new StringReader(tupleString)));
            Document xmlDoc = parser.getDocument();
            NodeList timeSteps = xmlDoc.getElementsByTagName("timestep");
            for (int i=0; i < timeSteps.getLength();i++) {
                Element timeStep = (Element) timeSteps.item(i);
                String timeStepValue = timeStep.getAttribute("time");
                NodeList vehicles = timeStep.getElementsByTagName("vehicle");
                for ( int j=0; j < vehicles.getLength(); j++) {
                    Element vehicle = (Element) vehicles.item(j);
                    collector.emit(new Values(timeStepValue, vehicle.getAttribute("id") , vehicle.getAttribute("speed") , vehicle.getAttribute("x"), vehicle.getAttribute("y")));
                }
            }
        } catch (SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timeStep","vehicleID","speed", "longitude","latitude"));
    }
}
