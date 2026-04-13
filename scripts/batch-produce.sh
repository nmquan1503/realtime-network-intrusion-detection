export PROJECT_PATH=$(pwd)
envsubst < k8s/simulator/batch_producer.yaml | kubectl apply -f -