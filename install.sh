mvn clean package

if [ $? -eq 0 ]; then

	HOST="USER@HOST"
	AUTH="-i PEM"

	SCP="scp "$AUTH
	SSH="ssh "$AUTH" "$HOST

	echo
	echo "Stoping oozie:"
	$SSH -t "/etc/init.d/oozie stop" > /dev/null

	echo
	echo "Ensuring dirs:"
	$SSH -t "mkdir -p /usr/lib/oozie/oozie-server-0.20/webapps/oozie/WEB-INF/lib/"

	echo
	echo "Updating JARs:"
	$SCP target/oozie-sge-0.0.1-SNAPSHOT.jar \
	     /Users/ataggart/.m2/repository/org/apache/commons/commons-exec/1.1/commons-exec-1.1.jar \
	     $HOST:/usr/lib/oozie/oozie-server-0.20/webapps/oozie/WEB-INF/lib/

	echo
	echo "Updating oozie config:"
	$SCP conf/oozie-site.xml $HOST:/etc/oozie/conf.dist/

	echo
	echo "Starting oozie:"
	$SSH -t "/etc/init.d/oozie start" > /dev/null

fi

