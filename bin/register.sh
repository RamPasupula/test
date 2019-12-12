java \
-Dlog4j.configuration=file:///Users/cb186046/IdeaProjects/edf/log4j.xml \
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9010 \
-Dcom.sun.management.jmxremote.local.only=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Dframework-conf.properties=/Users/cb186046/IdeaProjects/edf/framework-conf-cb.properties \
-Ddatabase.properties=/Users/cb186046/IdeaProjects/edf/database-cb.properties \
-Dinterface_spec.properties=/Users/cb186046/IdeaProjects/edf/interface_spec.properties \
-cp /Users/cb186046/IdeaProjects/edf/target/edf-master.jar:/Users/cb186046/IdeaProjects/edf/target/lib/* \
com.uob.edag.processor.RegistrationGenerator "$@"