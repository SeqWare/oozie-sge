mvn -Dmaven.test.skip=true clean package

HOST="SOME_USER@SOME_HOST"
AUTH="-i SOME_PEM"

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


NFS_EXAMPLE="/var/example" # Must match the value in workflow.xml

HDFS_ROOT="/user/oozie"
HDFS_EXAMPLE=$HDFS_ROOT"/example" # Must match the value in job.properties

echo
echo "Updating example:"
$SCP -r example $HOST:$NFS_EXAMPLE

echo
echo "Granting access to example:"
$SSH -t "chmod -R 777 "$NFS_EXAMPLE


echo
echo "Updating hadoop:"
$SSH -t "sudo -u oozie hadoop fs -rm -r "$HDFS_EXAMPLE" ;\
         sudo -u oozie hadoop fs -mkdir "$HDFS_EXAMPLE" ; \
         sudo -u oozie hadoop fs -put "$NFS_EXAMPLE"/workflow.xml "$HDFS_EXAMPLE"/workflow.xml"

echo
echo "Done!"



