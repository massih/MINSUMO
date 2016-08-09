zkServer.sh start ; (storm nimbus & storm supervisor & kafka-server-start.sh $KAFKA_HOME/config/server.properties &) ; sleep 15 ; jps
