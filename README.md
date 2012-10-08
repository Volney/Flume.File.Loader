Flume.File.Loader
=================

This is a working example of how to use Flume 1.1.0 to load files into hadoop.

##To Compile
Just go to the root project directory and type in "mvn clean install"

##To Run
Just go to a cluster that has Flume 1.1.0 installed and go to a directory with the flume.file.loader jar and type the following command.

flume-ng agent -C ./flume.file.loader-0.0.1-SNAPSHOT.jar -n agent1 -c conf -f BasicFileLoader.properties

