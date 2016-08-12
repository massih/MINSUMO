#!/bin/bash
read -p "Enter map folder name: " filename
gnome-terminal -x sh -c "storm jar ./dataCategorizer/target/data-categorizaer-storm-1.0-SNAPSHOT-jar-with-dependencies.jar com.minsumo.datacategorizer.DCtopology; bash"; gnome-terminal -x sh -c "./SumoReceiver/target/sumoReceiver; bash" ; gnome-terminal -x sh -c "sumo --fcd-output $HOSTNAME:12345  --fcd-output.geo -c $SUMO_HOME/maps/$filename/$filename.sumocfg; bash"
