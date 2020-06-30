Requirements/Technology used
1. Apache Spark(PySpark) - spark-2.4.3-bin-hadoop2.7
2. Docker
3. Python3

Instructions on executing the application

1. Unzip the file
2. Open terminal
3. cd into NYC-BikeStations/docker-spark-master
4. run command sudo docker run --rm -it -p 4040:4040 -v $(pwd)/count.py:/NYCBikeStations.py gettyimages/spark bin/spark-submit /NYCBikeStations.py


The output, CSV file, for the application would be in a folder called "output".  
To get the output on your local host machine the volume needs to be mounted in the Docker file and the arguements in the command in point(4) needs to be changed appropriately.