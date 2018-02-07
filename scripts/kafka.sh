# We start a hosted tools, mapped on our code
# Linux / Mac
docker run --rm -it --net=host landoop/fast-data-dev bash

# a topic for apache spark (key: IP-address)
kafka-topics --create --topic stop-bot-ip-requests --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

# a topic for lookup-service (key: URL-domain)
kafka-topics --create --topic stop-bot-url-domain --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

# run kafka-connect-spooldir
cd conf
connect-standalone connect-avro.properties kafka-connect-spooldir.properties

# run kafka-ignite-sink
cd conf
connect-standalone connect-string.properties kafka-connect-ignite.properties