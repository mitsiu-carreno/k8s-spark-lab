helm repo add minio https://charts.min.io/

helm upgrade \
--install minio minio/minio \
--namespace minio \
--create-namespace \
--cleanup-on-fail \
--set resources.requests.memory=512Mi \
--set replicas=1 \
--set persistence.enabled=false \
--set mode=standalone \
--set rootUser=user,rootPassword=password


kubectl -n minio port-forward pod/$(kubectl -n minio get pods -o jsonpath={".items[0].metadata.name"}) 9001

helm uninstall -n minio minio

helm repo remove minio

kubectl delete namespace minio
