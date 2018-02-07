# download and compile plugin https://github.com/jcustenborder/kafka-connect-spooldir
# or you can download https://mvnrepository.com/artifact/com.github.jcustenborder.kafka.connect/kafka-connect-spooldir/1.0.17
git clone git@github.com:jcustenborder/kafka-connect-spooldir.git
cd kafka-connect-spooldir
mvn clean package

# File-Stream-Spooldir plugin
docker run --rm -it --net=host \
  -v ${pwd}/kafka-connect/kafka-connect-spooldir/target/kafka-connect-target/usr/share/kafka-connect:/usr/local/share/kafka/plugins \
  -v ${pwd}/kafka-connect:/conf \
  -v /tmp/logs2:/tmp/logs2 \
  -v /tmp/logs2-finished:/tmp/logs2-finished \
  -v /tmp/logs2-errors:/tmp/logs2-errors \
  landoop/fast-data-dev bash