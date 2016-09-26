package com.minsumo.datacategorizer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.github.davidmoten.grumpy.core.Position;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.xerces.parsers.DOMParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by massih on 4/26/16.
 */

public class CategorizingBolt extends BaseRichBolt {

    private final String KAFKA_SERVER = "localhost:9092";
    private OutputCollector collector;
    private final double COVERAGE_AREA = 0.2;
    private RTree<String, Point> rsuTree;
    private String rsuString;
    private Producer<String, String> kafkaProducer;

    private static final Logger LOG = LoggerFactory.getLogger(CategorizingBolt.class);

    public CategorizingBolt(String buffer) {

        rsuString = buffer;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        try {
            rsuTree = makeTree();
        } catch (SAXException | IOException e) {
            e.printStackTrace();
        }

        kafkaSetup();
    }

    @Override
    public void execute(Tuple tuple) {
        /*
        "vehicleID"
        "speed"
        "longitude"
        "latitude"
        */
        Double lon = Double.parseDouble(tuple.getStringByField("longitude"));
        Double lat = Double.parseDouble(tuple.getStringByField("latitude"));
        Point vPoint = Point.create(lon, lat);
        List<Entry<String, Point>> rsuList = search(rsuTree, vPoint, COVERAGE_AREA).toList().toBlocking().single();
        String chosenRSU = "";
        if (rsuList.size() > 0){
            chosenRSU = rsuList.get(0).value();
            if (rsuList.size() > 1){
                Double minDistance = COVERAGE_AREA;
                Position vPos = Position.create(lat, lon);
                for (Entry<String, Point> rsu : rsuList){
                    Double distance = vPos.getDistanceToKm( Position.create(rsu.geometry().y(), rsu.geometry().x()) );
                    if ( distance <= minDistance ){
                        chosenRSU = rsu.value();
                        minDistance = distance;
                    }
                }
            }
            //kafkaProducer.send(new ProducerRecord<String, String>(chosenRSU, tuple.toString() ));
            //System.out.println("*******number of Found RSU: " + rsuList.size() + " chosen one:"+ chosenRSU + " *******");
        }
        String result = tuple.getStringByField("firstTimestamp") + ";" + String.valueOf(System.currentTimeMillis());
        kafkaProducer.send(new ProducerRecord<String, String>("toEvaluate", result ));
        //collector.emit(tuple, new Values(Long.parseLong(tuple.getStringByField("firstTimestamp")) ,System.currentTimeMillis()) );
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("timeStep","firstTimestamp","secondTimestamp","vehicleID","speed", "longitude","latitude"));
        declarer.declare(new Fields("firstTimestamp", "secondTimestamp"));
    }

    private void kafkaSetup() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_SERVER);
        props.put("acks", "0");
        props.put("retries", 0);
        props.put("batch.size", 0);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("delete.topic.enable", "true");
        kafkaProducer = new KafkaProducer<>(props);
    }

    private RTree<String, Point> makeTree() throws IOException, SAXException {
        DOMParser parser = new DOMParser();
        parser.parse(new InputSource(new StringReader(rsuString)));
        RTree<String, Point> tree = RTree.star().create();
        Document xmlDoc = parser.getDocument();
        NodeList rsus  = xmlDoc.getElementsByTagName("rsu");
        Double lat, lon;
        String rsuID;
        //System.out.println("*************NUMBER OF RSUS: " + rsus.getLength());
        for (int i=0; i < rsus.getLength(); i++) {
            Element rsu = (Element) rsus.item(i);
            NodeList elements = rsu.getChildNodes();
            //System.out.println("*************RSU CHILDES SIZE:" + elements.getLength());
            rsuID = rsu.getElementsByTagName("id").item(0).getTextContent();
            lat = Double.parseDouble(rsu.getElementsByTagName("latitude").item(0).getTextContent());
            lon = Double.parseDouble(rsu.getElementsByTagName("longitude").item(0).getTextContent());
            //lon = Double.parseDouble(rsu.getAttribute("longitude"));
            //System.out.println("*************ELEMENT TO CREATE TREE:" + rsuID + " - " + lat +" , " +lon);
            tree = tree.add(rsuID, Geometries.pointGeographic(lon, lat));
        }
        return tree;
    }

    private <T> Observable<Entry<T, Point>> search(RTree<T, Point> tree, Point lonLat,
                                                   final double distanceKm) {
        // First we need to calculate an enclosing lat long rectangle for this
        // distance then we refine on the exact distance
        final Position from = Position.create(lonLat.y(), lonLat.x());
        Rectangle bounds = createBounds(from, distanceKm);

        return tree
                // do the first search using the bounds
                .search(bounds)
                // refine using the exact distance
                .filter(new Func1<Entry<T, Point>, Boolean>() {
                    @Override
                    public Boolean call(Entry<T, Point> entry) {
                        Point p = entry.geometry();
                        Position position = Position.create(p.y(), p.x());
                        return from.getDistanceToKm(position) < distanceKm;
                    }
                });
    }

    private Rectangle createBounds(final Position from, final double distanceKm) {
        // this calculates a pretty accurate bounding box. Depending on the
        // performance you require you wouldn't have to be this accurate because
        // accuracy is enforced later
        Position north = from.predict(distanceKm, 0);
        Position south = from.predict(distanceKm, 180);
        Position east  = from.predict(distanceKm, 90);
        Position west  = from.predict(distanceKm, 270);

        return Geometries.rectangle(west.getLon(), south.getLat(), east.getLon(), north.getLat());
    }
}
