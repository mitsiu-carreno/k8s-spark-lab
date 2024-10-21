# Helm installation
We must add the helm repo in this case I selected the name **minio**
```
helm repo add minio https://charts.min.io/
```

Once the helm repo is added we can install the chart **minio** defined in the bitnami repo in the `minio` namespace, additionally we setup some minor tweaks, notice that **credentials are hardcoded, this should be avoided in a productive environment**.
```  
helm upgrade \
--install minio minio/minio \
--version 5.3.0 \
--namespace minio \
--create-namespace \
--cleanup-on-fail \
--set resources.requests.memory=512Mi \
--set replicas=1 \
--set persistence.enabled=false \
--set mode=standalone \
--set rootUser=user,rootPassword=password
```

# Accessing web console
We can forward the service minio-console which by default is available on port 9001 in the kubernetes cluster
```
kubectl -n minio port-forward svc/minio-console 9001
```
Also we can forward the first pod in the cluster, this is a worse method because it relias on the pod (which is volatile) instead of the service, but I leave the command for educational reasons
``` 
kubectl -n minio port-forward pod/$(kubectl -n minio get pods -o jsonpath={".items[0].metadata.name"}) 9001
```

# Destroy spark cluster
Once we are done with our minio cluster, we can destroy it with the following commands
```
# Destroy helm chart and delete all associated kubernetes resources
helm uninstall -n minio minio
# Remove the minio repo
helm repo remove minio
# Remove all dangling resources
kubectl delete namespace minio
```
