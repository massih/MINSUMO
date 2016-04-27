package com.minsumo.datacategorizer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.UUID;

/**
 * Created by massih on 4/12/16.
 */
public class DCtopology {
    private static final Logger LOG = LoggerFactory.getLogger(DCtopology.class);

    public static void main(String args[]){
        String topicName = "unprocessed";
        TopologyBuilder builder = new TopologyBuilder();

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(5);
        //KafkaConfig
        Broker broker = new Broker("localhost", 9092);
        GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
        partitionInfo.addPartition(0,broker);
        StaticHosts staticHosts = new StaticHosts(partitionInfo);
        SpoutConfig spoutConfig = new SpoutConfig(staticHosts, topicName, "/brokers", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        builder.setSpout("unprocessedKafkaSpout", new KafkaSpout(spoutConfig), 1);
        builder.setBolt("filteringBolt", new FilteringBolt(), 1).shuffleGrouping("unprocessedKafkaSpout");
        builder.setBolt("categorizingBolt", new CategorizingBolt(), 1).shuffleGrouping("filteringBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("middlewareLayer", conf, builder.createTopology());
    }
}
