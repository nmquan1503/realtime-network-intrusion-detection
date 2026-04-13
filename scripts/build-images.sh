docker build -f docker/Dockerfile.batch -t mquan1503/bigdata-batch:latest .
# docker build -f docker/Dockerfile.simulator -t mquan1503/bigdata-simulator:latest .
# docker build -f docker/Dockerfile.batch -t mquan1503/bigdata-spark:latest .

docker push mquan1503/bigdata-batch:latest
# docker push mquan1503/bigdata-simulator:latest
# docker push mquan1503/bigdata-spark:latest