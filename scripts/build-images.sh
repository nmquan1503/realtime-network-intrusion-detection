docker build -f docker/Dockerfile.batch -t mquan1503/bigdata-batch:latest .
docker build -f docker/Dockerfile.simulator -t mquan1503/bigdata-simulator:latest .
docker build -f docker/Dockerfile.streaming -t mquan1503/bigdata-streaming:latest .
docker build -f docker/Dockerfile.dashboard -t mquan1503/bigdata-dashboard:latest .
# docker build -f docker/Dockerfile.batch -t mquan1503/bigdata-spark:latest .
docker build -f docker/Dockerfile.airflow -t mquan1503/bigdata-airflow:latest .

docker push mquan1503/bigdata-batch:latest
docker push mquan1503/bigdata-simulator:latest
docker push mquan1503/bigdata-streaming:latest
docker push mquan1503/bigdata-dashboard:latest
# docker push mquan1503/bigdata-spark:latest
docker push mquan1503/bigdata-airflow:latest
