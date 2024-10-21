# Helm installation
We must add the helm repo in this case I selected the name **bitnami**
```
helm repo add bitnami https://charts.bitnami.com/bitnami
```

Once the helm repo is added we can install the chart **spark** in the bitnami repo in the `spark-cluster` namespace, additionally we setup some minor tweaks defined in `values.yml`.
```
helm upgrade \
--install spark-cluster bitnami/spark \
--version 9.2.12 \
--namespace spark-cluster \
--create-namespace \
--cleanup-on-fail \
--values values.yml
```

# Kubernetes monitoring
We can monitor the spark cluster creation with
```
watch -d kubectl -n spark-cluster get all
```

Also we can forward service <namespace>-master-svc which by default is available on port 80 in the kubernetes cluster to port 8080 in the host
```
kubectl port-forward --namespace spark-cluster svc/spark-cluster-master-svc 8080:80
```

As well we can forward ports from the pods specifically with the following commands
```
kubectl -n spark-cluster port-forward spark-cluster-worker-0 8081:8080
kubectl -n spark-cluster port-forward spark-cluster-master-0 4040:4040
```

# Launcing applications to the spark-cluster
To launch an aplication into the spark cluster there are several pre-requirements that must be meet:
1. The app must be a python file (.py, .zip, .egg) or an app built into an assembly jar (or “uber” jar) containing your code and its dependencies (this can be achieved using either **sbt** or **maven**).
2. The app submited must be present in the master node.
3. All external data used by the app should be duplicated into all worker nodes, matching both paths and names.

## Launching default example
To start simple, we can run an app already present in the base image used to build the spark cluster, so it's already present in all spark cluster nodes, the path to the app in each node is `/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.3.jar`.          
We must pass the master URL which in kubernetes is composed of:        
`<service-name>.<namespace>.svc.cluster.local:<service-port>`         
Also we must specify the class that contains the main function in the application
```
kubectl exec -n spark-cluster -it spark-cluster-master-0 -- ./bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://spark-cluster-master-svc.spark-cluster.svc.cluster.local:7077 \
./examples/jars/spark-examples_2.12-3.5.3.jar 1000
```

## Launch custom application
As mentioned we can submit our own applications, in this case we will launch basic python application named `submit-job.py` notice that it's using a s3 bucket as input data, so update the input dir accordingly either by setting up other s3 service (like minio described in this repo or other like AWS s3)      
In order to achieve it we have to copy the app into the spark-cluster, we can use the following command to copy `submit-job.py` from host to spark-master-node
```
kubectl -n spark-cluster cp submit-job.py spark-cluster-master-0:/tmp/submit-job.py
```
Once the app file is in the spark cluster, we can submit the job, in this case I am **hardcoding s3 credentials, this is a bad practice** and should be avoided, instead kubernetes secrets should be implemented.
```
kubectl -n spark-cluster exec -it spark-cluster-master-0 -- ./bin/spark-submit \
--master spark://spark-cluster-master-svc.spark-cluster.svc.cluster.local:7077 \
--conf spark.hadoop.fs.s3a.endpoint=http://minio.minio.svc.cluster.local:9000 \
--conf spark.hadoop.fs.s3a.access.key=user \
--conf spark.hadoop.fs.s3a.secret.key=password \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.num.executors=1 \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=2G \
/tmp/submit-job.py
```

# Change resources in the spark cluster
It is posible to update the resources specs for the spark cluster even when it's already running, we can update our `values.yml` to version our spark cluster or add **--set** configurations, the full customization options can be found at the [documentation](https://github.com/bitnami/charts/tree/main/bitnami/spark/#parameters)
```
helm upgrade \
--install spark-cluster bitnami/spark \
--version 9.2.12 \
--namespace spark-cluster \
--create-namespace \
--cleanup-on-fail \
--values values.yml \
--set worker.replicaCount=5
```

# Destroy spark cluster
Once we are done with our spark cluster, we can destroy it with the following commands
```
# Destroy helm chart and delete all associated kubernetes resources
helm uninstall -n spark-cluster spark-cluster
# Remove the bitnami repo 
helm repo remove bitnami
# Remove all dangling resources
kubectl delete namespace spark-cluster
```

# Usefull references
Your best friend is the official documentation, but also you might find helpful:
https://spark.apache.org/docs/latest/running-on-kubernetes.html
https://github.com/bitnami/charts/tree/main/bitnami/spark

