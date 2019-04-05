# Installing and Running PARJ

Contact d.bilidas at di.uoa.gr for any issues

## Preprocessing

Use the Data Loader from this github project: https://github.com/dbilid/rdf-exp
This will create the dictionary and will encode the RDF data. After succesfully running the data loader, a file with name rdf.db will be created in the specified encoded_file_path.

## Installing PARJ

PARJ has been tested in DEBIAN 8 and UBUNTU 16.04 and 18.04.

### Prerequisites
-JAVA 8 with JAVA_HOME environment variable set. (e.g. sudo apt install openjdk-8-jdk)
set default version (sudo update-alternatives --config java)
and set JAVA_HOME: (e.g. JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"
export JAVA_HOME)

-build-essential (e.g. sudo apt-get install build-essential)

-libgoogle-perftools-dev (e.g. sudo apt-get install  libgoogle-perftools-dev)

-pkg-config (e.g. sudo apt-get install pkgconf)

-libgtk2 (e.g. sudo apt-get install libgtk2.0-dev)

-libsqlite3-dev (e.g. sudo apt-get install libsqlite3-dev)

-libraptor2-dev (e.g. sudo apt-get install libraptor2-dev)

### Build

-Clone this project (e.g. git clone https://github.com/dbilid/PARJ.git)

-Move to directory and run mvn build as follows in order to build PARJ JAVA wrapper:

cd PARJ/

mvn clean install -DskipTests=true

-Run the following in order to build the C loadable extension:

gcc -O3 -o parj.so parj.c -lpthread  -lraptor2 -fPIC -shared `pkg-config --cflags --libs glib-2.0`;

-Move the loadable extension to the target directory:

mv mv parj.so exareme-distribution/target/exareme-distribution-0.1-SNAPSHOT-default/lib/exareme/

### Run

-change to target directory (cd exareme-distribution/target/exareme-distribution-0.1-SNAPSHOT-default/lib/exareme/) and run: java -cp "exareme-master-0.1-SNAPSHOT.jar:exareme-utils-0.1-SNAPSHOT.jar:external/*" madgik.exareme.master.importer.QueryTester /path/to/directory/

where /path/to/directory/ is the encoded_file_path used in the preprocessing

On startup you will be asked to provide the following information:

-Load Dictionary in memory: If yes the ID to URI dictionary will be loaded in memory. If no it will be kept on disk. This setting does not control if dictionary lookups will be finally added to the query. This option is controlled by the next setting.

-Use dictionary lookups for results (includes result tuple construction): If yes dictionary lookups will be performed during query execution. If dictionary is loaded in memory according to the previous setting, then memory lookups will be performed, otherwise disk will be accessed. If disk is accessed you can expect a big difference between cold cache and warm cache times. This option does not affect result printing, something that is controlled by next option.

-Print results: If yes results will be printed. If dictionary lookups are enabled then the URIs and literals will be printed, otherwise the number codes will be printed.

-Execute queries from File: If yes, after data loading you will be asked to provide the filepath of the file that contains the queries. Each query must be in a single line in the file. Lines with less than 30 characters will be ignored. Each query will be executed 11 times. The last 10 times will be taken into consideration for the average execution time. After all queries in the file have been executed the average execution time for each query, as well as the total average and geomean will be printed. If this setting is no, then you will be asked to directly give as input a query text in a single line (that is query text must not contain new line characters). After execution the number of results and total execution time in ms will be printed and you will be asked to give another query for execution.

-give number of threads: Insert the number of threads that would be used for each query

