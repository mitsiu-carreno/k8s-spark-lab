helm repo add bitnami https://charts.bitnami.com/bitnami

helm upgrade \
--install spark-cluster bitnami/spark \
--namespace spark-cluster \
--create-namespace \
--cleanup-on-fail



watch -d kubectl -n spark-cluster get pods

kubectl port-forward --namespace spark-cluster svc/spark-cluster-master-svc 8080:80

kubectl -n spark-cluster port-forward spark-cluster-worker-0 8081:8080

kubectl -n spark-cluster port-forward spark-cluster-master-0 4040:4040



kubectl exec -n spark-cluster -it spark-cluster-master-0 -- ./bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://spark-cluster-master-0.spark-cluster-headless.spark-cluster.svc.cluster.local:7077 \
./examples/jars/spark-examples_2.12-3.5.3.jar 1000



kubectl -n spark-cluster cp submit-job.py spark-cluster-master-0:/tmp/submit-job.py

kubectl -n spark-cluster exec -it spark-cluster-master-0 -- ./bin/spark-submit \
--master spark://spark-cluster-master-0.spark-cluster-headless.spark-cluster.svc.cluster.local:7077 \
--conf spark.hadoop.fs.s3a.endpoint=http://minio.minio.svc.cluster.local:9000 \
--conf spark.hadoop.fs.s3a.access.key=user \
--conf spark.hadoop.fs.s3a.secret.key=password \
--conf spark.hadoop.fs.s3a.path.style.access=true \
/tmp/submit-job.py


helm upgrade \
--install spark-cluster bitnami/spark \
--namespace spark-cluster \
--create-namespace \
--cleanup-on-fail \
--set worker.replicaCount=5

helm uninstall -n spark-cluster spark-cluster

helm repo remove bitnami

kubectl delete namespace spark-cluster
