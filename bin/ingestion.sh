java -Djava.io.tmpdir=/tmp -Dlog4j.configuration=file:///home/ownedapcsg/EDF/conf/log4j.xml -Dframework-conf.properties=/home/ownedapcsg/EDF/conf/framework-conf.properties -Ddatabase.properties=/home/ownedapcsg/EDF/conf/database.properties -Ddateformat.properties=/home/ownedapcsg/EDF/conf/dateformat.properties -cp edf-01.01.09.jar:/home/ownedapcsg/EDF/lib/* com.uob.edag.processor.BaseProcessor "$@"