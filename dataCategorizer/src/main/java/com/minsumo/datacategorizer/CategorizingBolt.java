package com.minsumo.datacategorizer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.github.davidmoten.grumpy.core.Position;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.geometry.Rectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by massih on 4/26/16.
 */

public class CategorizingBolt extends BaseRichBolt {

    private OutputCollector collector;
    private final double COVERAGE_AREA = 0.25;
    private BufferedReader bufferedReader;
    private RTree<String, Point> rsuTree;

    private static final Logger LOG = LoggerFactory.getLogger(CategorizingBolt.class);

    public CategorizingBolt(String buffer) {
        //bufferedReader = new BufferedReader(buffer);
        System.out.println("************ size of tree ====== " +buffer.split("\n").length+" ***********");

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        //rsuTree = makeTree();
        //System.out.println("************ size of tree ====== " +rsuTree.size()+" ***********");
    }

    @Override
    public void execute(Tuple tuple) {
               /*
        "timeStep"
        "vehicleID"
        "speed"
        "longitude"
        "latitude"
        */
        Double lon = Double.parseDouble(tuple.getString(3));
        Double lat = Double.parseDouble(tuple.getString(4));
        Point vPoint = Point.create(lon, lat);
        List<Entry<String, Point>> rsuList = search(rsuTree, vPoint, COVERAGE_AREA).toList().toBlocking().single();
        System.out.println("*******Found RSU num: " + rsuList.size() + " *******");
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private RTree<String, Point> makeTree(){
        RTree<String, Point> tree = RTree.star().create();
        String[] idLatLon;
        String id;
        double lat,lon;
        String line = null;
        try {
            line = bufferedReader.readLine();
            while (line != null) {
                idLatLon = line.split(",");
                //System.out.println(idLatLon[0] + " " + idLatLon[1] + " " + idLatLon[2]);
                id = idLatLon[0];
                lat = Double.parseDouble(idLatLon[1]);
                lon = Double.parseDouble(idLatLon[2]);
                tree = tree.add(id, Geometries.pointGeographic(lon, lat));
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
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
        Position east = from.predict(distanceKm, 90);
        Position west = from.predict(distanceKm, 270);

        return Geometries.rectangle(west.getLon(), south.getLat(), east.getLon(), north.getLat());
    }
}
