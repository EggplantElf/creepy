LOG_FILE=/home/users0/xiangyu/creepy/tmp/pid.log
SCRIPT=/home/users0/xiangyu/creepy/crawl.py

while read pid; do
  echo 'killed' $pid
  kill -9 $pid
done < $LOG_FILE
rm -rf $LOG_FILE

nohup python $SCRIPT -l de > /dev/null &
echo $! >> $LOG_FILE
nohup python $SCRIPT -l tr > /dev/null &
echo $! >> $LOG_FILE

