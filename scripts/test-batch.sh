kubectl apply -f k8s/batch/consumer.yaml
sleep 60
kubectl apply -f k8s/batch/preprocessing.yaml
sleep 60
kubectl apply -f k8s/batch/trainer.yaml