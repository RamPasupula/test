# EDF

## Teradata's Enterprise Data Framework for UOB

### Missing external libraries from Maven repository

The 2 following libraries are needed to build the project but they are missing from Maven repositories and need to be installed manually

* Oracle JDBC driver (ojdbc6.jar)
* Impala JDBC driver (ImpalaJDBC41.jar)

These 2 jar files can be downloaded respectively to
* http://www.oracle.com/technetwork/apps-tech/jdbc-112010-090769.html
* https://www.cloudera.com/downloads/connectors/impala/jdbc/2-5-36.html

Then add them to you local maven repository:

```mvn install:install-file -Dfile=/path/to/ojdbc6.jar -DgroupId=oracle.jdbc.driver.OracleDriver -DartifactId=ojdbc -Dversion=6 -Dpackaging=jar```

```mvn install:install-file -Dfile=/path/to/ImpalaJDBC41.jar -DgroupId=com.cloudera.impala.jdbc41.Driver -DartifactId=impala-jdbc -Dversion=2.5.36 -Dpackaging=jar```

* TD Interface jar (tdinterfaces.jar) 
** this jar file is for hook invoke system.

```mvn install:install-file -Dfile=/path/to/tdinterfaces.jar -DgroupId=com.teradata.hook -DartifactId=tdinterfaces -Dversion=0.0.1 -Dpackaging=jar```

### Build the project

```mvn install -DskipTests```

### Package the project into a tarball

```mvn package -DskipTests```

### Local development
Developing and testing locally requires at least a running instance of Oracle (metastore of the framewok). Docker image can be used to run a local instance.
Follow the steps in the link below to run Oracle locally.

https://github.com/wnameless/docker-oracle-xe-11g

Metastore user/schema and tables can be deployed using *deploy_metastore.sh* script under dev/
```./deploy_metastore.sh username password```

SQL files for the metastore creation and initialisation can be found under data/metastore/

Don't forget to update database.properties with the username and password for Oracle connection!


Although the registration (registration.sh) requires Oracle running, 
the full ingestion (Landing area -> T1 -> T1.1) requires HDFS, YARN, Hive, Impala and Informatica BDM.
It is however possible to bypass these steps passing the following parameter to the JVM options `-DlocalDevEnv=true`

#### Registration example

```
java \
-Dlog4j.configuration=file:///path/to/log4j.properties \
-Dframework-conf.properties=/path/to/framework-conf.properties \
-Ddatabase.properties=/path/to/database.properties \
-cp /Users/cb186046/IdeaProjects/edf/target/edf-master.jar:/Users/cb186046/IdeaProjects/edf/target/lib/* \
com.uob.edag.processor.RegistrationGenerator \
-r \
"/path/to/EDW_LNS_Interface_File_Format_Specification_Final_V1.02_TD.xlsx" \
"/path/to/UOB_EDAG_Process_Specification_LNS_TBLNAME.xlsx" \
-f
```

#### Ingestion example

```
java \
-Dlog4j.configuration=file:///home/ownedapcsg/EDF/conf/log4j.properties \
-Dframework-conf.properties=/home/ownedapcsg/EDF/conf/framework-conf.properties \
-Ddatabase.properties=/home/ownedapcsg/EDF/conf/database.properties \
-cp edf-masterjar:/home/ownedapcsg/EDF/lib/* com.uob.edag.processor.BaseProcessor \
-i FI_LNS_LNASAPRD_D01 2018-03-12 SG -f
```

#### Registration T1.4 example

```
java \
-Dlog4j.configuration=file:///path/to/log4j.properties \
-Dframework-conf.properties=/path/to/framework-conf.properties \
-Ddatabase.properties=/path/to/database.properties \
-cp /Users/cb186046/IdeaProjects/edf/target/edf-master.jar:/Users/cb186046/IdeaProjects/edf/target/lib/* \
com.uob.edag.processor.GenerateProcessSQL \
-r \
"/path/to/UOB_EDAG_FFR_1.4_Process_Specificaiton_V0.01_20170509.xlsx" \
```

#### Ingestion example

```
java \
-Dlog4j.configuration=file:///home/ownedapcsg/EDF/conf/log4j.properties \
-Dframework-conf.properties=/home/ownedapcsg/EDF/conf/framework-conf.properties \
-Ddatabase.properties=/home/ownedapcsg/EDF/conf/database.properties \
-cp edf-masterjar:/home/ownedapcsg/EDF/lib/* com.uob.edag.processor.DataLoadingProcessor \
-p LD_BWC_CFZPCC_D01_SG SG -b 2017-05-12 -f
```

## How-tos

### How to create a branch to another branch

```git checkout -b release-n develop```

### How to revert locally and remotely to a certain commit

```git reset --hard commit-sha```

```git push -f origin master```

### How to revert locally to a commit and create a branch

```git checkout commit-sha```

```git checkout -b branch-name commit-sha```

```git push origin branch-name```

### How to merge branch back to master

```git checkout master```

```git pull origin master```

```git merge branch_name```

```git push origin master```


### How to monitor resources usage with JMX

Add the following parameters to the JVM:

```
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9010 \
-Dcom.sun.management.jmxremote.local.only=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
```



Then run ```jconsole``` from the client.


