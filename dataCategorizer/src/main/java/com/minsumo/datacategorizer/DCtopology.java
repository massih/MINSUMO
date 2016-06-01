package com.minsumo.datacategorizer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.github.davidmoten.rtree.geometry.Geometries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.io.*;
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
    private final static String EVALUATION_BOLT         = "evaluationBolt";
    private final static String ZK_ROOT                 = "/brokers";
    private final static String rsuFile                 = "RSU_t.xml";

    public static void main(String args[]) throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(10);
        //KafkaConfig
        Broker broker = new Broker(KAFKA_HOST, KAFKA_PORT);
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        partitionInfo.addPartition(0,broker);
        StaticHosts staticHosts = new StaticHosts(partitionInfo);
        SpoutConfig spoutConfig = new SpoutConfig(staticHosts, KAFKA_TOPIC_NAME, ZK_ROOT, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        //Reading RSU file
        ClassLoader classLoader = DCtopology.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(rsuFile);
        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(inputStream));
        String temp = bufferReader.readLine();
        String res = "";
        while (temp != null) {
            res += temp+"\n";
            temp = bufferReader.readLine();
        }

        //TOPOLOGY COMPONENTS
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(spoutConfig), 5);
        builder.setBolt(FILTERING_BOLT, new FilteringBolt(), 5).shuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(CATEGORIZING_BOLT, new CategorizingBolt(res), 5).shuffleGrouping(FILTERING_BOLT);
        builder.setBolt(EVALUATION_BOLT, new EvaluationBolt(), 3).shuffleGrouping(CATEGORIZING_BOLT);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
    }


}
