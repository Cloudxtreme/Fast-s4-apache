#!/bin/bash

while true; do

	URLS_BBC=`wget http://feeds.bbci.co.uk/news/rss.xml -o /dev/null -O - | grep '<link>' | sed 's/.*<link>//' | sed 's/[\?#].*//' | sed 's/<\/link>.*//' | egrep '/news/.' | uniq | head -n 30 | xargs` 
	URLS_CNN=`wget http://rss.cnn.com/rss/edition.rss -o /dev/null -O - | grep '<link>' | sed 's/.*<link>//' | sed 's/[\?#].*//' | sed 's/<\/link>.*//' | sed 's/edition.cnn.com/www.cnn.com/' | sort --field-sep=/ -r -nk4,6 | uniq | head -n 30 | xargs`

	# Minimum 10
	$JAVA_HOME/bin/java -cp `/bin/ls $S4_IMAGE/s4-apps/rtsm-visit/lib/*.jar | xargs | sed 's/ /:/g'` qa.qcri.rtsm.track.InsertDummyVisits --siteid www.bbc.co.uk --nvisits 10 $URLS_BBC 
	$JAVA_HOME/bin/java -cp `/bin/ls $S4_IMAGE/s4-apps/rtsm-visit/lib/*.jar | xargs | sed 's/ /:/g'` qa.qcri.rtsm.track.InsertDummyVisits --siteid www.cnn.com --nvisits 10 $URLS_CNN

	# Optionally up to 3 visits more
	$JAVA_HOME/bin/java -cp `/bin/ls $S4_IMAGE/s4-apps/rtsm-visit/lib/*.jar | xargs | sed 's/ /:/g'` qa.qcri.rtsm.track.InsertDummyVisits --siteid www.bbc.co.uk --randomize --nvisits 3 $URLS_BBC 
	$JAVA_HOME/bin/java -cp `/bin/ls $S4_IMAGE/s4-apps/rtsm-visit/lib/*.jar | xargs | sed 's/ /:/g'` qa.qcri.rtsm.track.InsertDummyVisits --siteid www.cnn.com --randomize --nvisits 3 $URLS_CNN

	sleep 60
done