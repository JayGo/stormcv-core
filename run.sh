cd /home/jkyan/workspace/java/stormcv/stormcv/
#1:output dir 2: topic 3:spout 4:grey 5:ftp
storm jar /home/jkyan/workspace/java/stormcv/stormcv/target/stormcv-0.7.1-jar-with-dependencies.jar  nl.tno.stormcv.topology.KafkaTestTopology -cl -d $1 -n1 $3 -n2 $4 -n3 $5 -t $2
