java -Dlog4j.configuration=file:///home/ownedapcsg/EDF/conf/log4j.xml -Dframework-conf.properties=/home/ownedapcsg/EDF/conf/framework-conf.properties -Ddatabase.properties=/home/ownedapcsg/EDF/conf/database.properties -cp "/home/ownedapcsg/EDF/lib/*" com.uob.edag.processor.MonthlyBusinessCalendarProcessor "$@"