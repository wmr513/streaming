HOME=$STREAMING_HOME

CLASSPATH=$HOME/src/main/resources
CLASSPATH=$CLASSPATH:$HOME/bin
CLASSPATH=$CLASSPATH:$HOME/lib/commons-io-1.2.jar
CLASSPATH=$CLASSPATH:$HOME/lib/rabbitmq-client.jar
CLASSPATH=$CLASSPATH:$HOME/lib/kafka-clients-0.10.2.0.jar
CLASSPATH=$CLASSPATH:$HOME/lib/log4j-1.2.17.jar
CLASSPATH=$CLASSPATH:$HOME/lib/commons-logging.jar
CLASSPATH=$CLASSPATH:$HOME/lib/slf4j-api-1.7.21.jar
CLASSPATH=$CLASSPATH:$HOME/lib/slf4j-log4j12-1.7.21.jar

java -cp $CLASSPATH kafka.SymbolAnalyzer
