# Helm installation
We must add the helm repo in this case I selected the name **jupyterhub**
```
helm repo add jupyterhub https://hub.jupyter.org/helm-chart/
helm repo update
```

Once the helm repo is added we can install the chart **jupyterhub** defined in the jupyterhub repo in the `jupyterhub` namespace, additionally we setup some minor tweaks defined in `values.yml`.
***Important: For some unknown reason to my knowleadge, I have trouble creating and tweaking the chart, so I have to do it in two steps, but feel free to fix this and send me the PR***
```
helm upgrade \
--install jupyterhub jupyterhub/jupyterhub \
--version 3.3.8 \
--namespace jupyterhub \
--create-namespace \
--cleanup-on-fail \
--set prePuller.hook.enabled=false \
--set singleuser.storage.type=none

helm upgrade \
--install jupyterhub jupyterhub/jupyterhub \
--version 3.3.8 \
--namespace jupyterhub \
--create-namespace  \
--cleanup-on-fail \
--values values.yml
```

# Accessing web console
We can forward the service proxy-public which is available on port 80 in the kubernetes cluster to port 9080 in the host
```
kubectl --namespace=jupyterhub port-forward service/proxy-public 9080:http
```

# If jupyter is spark driver
As we are running jupyterhub to provide a driver for our spark-cluster it's worth mentioning that once an app is running spark enables port 4040 to access spark web ui, if our notebook proxy spark host to our notebook host, we can access forwarding in a similar command but replacing `jupyter-mit` with the pod that's running the notebook
```
kubectl --namespace=jupyterhub port-forward jupyter-mit 4040:4040
```

# Destroy jupyterhub cluster
Once we are done with our jupyterhub cluster, we can destroy it with the following commands
```
# Destroy helm chart and delete all associated kubernetes resources
helm uninstall -n jupyterhub jupyterhub
# Remove jupyterhub repo
helm repo remove jupyterhub
# Remove all dangling resources
kubectl delete namespace jupyterhub
# Jupyterhub by default creates some persisten volumes you can list them with
kubectl get pvc -A
kubectl get pv
# They can be removed with 
kubectl delete -n jupyterub pvc <name>
kubectl delete pv <name>
```
