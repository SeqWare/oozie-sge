echo "Removing test-support dirs/files, if they exist..."

sudo rm -rf test-support

echo "Creating test-support dirs/files"

mkdir test-support
cd test-support

mkdir -m 333 workDirNoRead

mkdir -m 555 workDirNoWrite

touch workDirNotDir

mkdir workDir
cd workDir

mkdir scriptNotFile.sh

touch scriptNoRead.sh
chmod 333 scriptNoRead.sh

touch scriptNoExec.sh
chmod 666 scriptNoExec.sh

echo "echo \"Your job foobar123 (\\\"scriptOk.sh\\\") has been submitted\"" > scriptOk.sh
chmod 777 scriptOk.sh

echo "echo \"oops\"" > scriptBadResult.sh
chmod 777 scriptBadResult.sh
