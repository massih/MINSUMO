package com.minsumo.datacategorizer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

/**
 * Created by massih on 4/12/16.
 */
public class DCtopology {
    private static final Logger LOG = LoggerFactory.getLogger(DCtopology.class);
    private final static int    KAFKA_PORT              = 9092;
    private final static String KAFKA_HOST              = "localhost";
    private final static String KAFKA_TOPIC_NAME        = "unprocessed";
    private final static String TOPOLOGY_NAME           = "middlewareLayer";
    private final static String KAFKA_SPOUT             = "unprocessedKafkaSpout";
    private final static String FILTERING_BOLT          = "filteringBolt";
    private final static String CATEGORIZING_BOLT       = "categorizingBolt";
    private final static String ZK_ROOT                 = "/brokers";

    public static void main(String args[]){
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(5);
        //KafkaConfig
        Broker broker = new Broker(KAFKA_HOST, KAFKA_PORT);
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        partitionInfo.addPartition(0,broker);
        StaticHosts staticHosts = new StaticHosts(partitionInfo);
        SpoutConfig spoutConfig = new SpoutConfig(staticHosts, KAFKA_TOPIC_NAME, ZK_ROOT, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        //Reading RSU file
        RTree<String, Point> tree = makeTree("RSUs.txt");
        conf.put("treeObject", tree);
        //TOPOLOGY COMPONENTS
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(spoutConfig), 1);
        builder.setBolt(FILTERING_BOLT, new FilteringBolt(), 1).shuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(CATEGORIZING_BOLT, new CategorizingBolt(), 1).shuffleGrouping(FILTERING_BOLT);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
    }

    private static RTree<String, Point> makeTree(String path){
        RTree<String, Point> tree = RTree.star().create();
        try (BufferedReader bufferReader = new BufferedReader(new FileReader(path)) ){
            String[] idLatLon;
            String id;
            double lat,lon;
            String line = bufferReader.readLine();
            while (line != null) {
                idLatLon = line.split(",");
                //System.out.println(idLatLon[0] + " " + idLatLon[1] + " " + idLatLon[2]);
                id = idLatLon[0];
                lat = Double.parseDouble(idLatLon[1]);
                lon = Double.parseDouble(idLatLon[2]);
                tree = tree.add(id, Geometries.pointGeographic(lon, lat));
                line = bufferReader.readLine();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        return tree;
    }
}
