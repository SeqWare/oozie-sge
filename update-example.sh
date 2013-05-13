mvn -Dmaven.test.skip=true clean package

HOST="USER@HOST"
AUTH="-i PEM"

SCP="scp "$AUTH
SSH="ssh "$AUTH" "$HOST

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



