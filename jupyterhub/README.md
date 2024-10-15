helm repo add jupyterhub https://hub.jupyter.org/helm-chart/

helm repo update

helm upgrade \
--install jupyterhub jupyterhub/jupyterhub \
--namespace jupyterhub \
--create-namespace \
--cleanup-on-fail \
--set singleuser.storage.type=none 

helm upgrade \
--install jupyterhub jupyterhub/jupyterhub \
--namespace jupyterhub \
--create-namespace  \
--cleanup-on-fail \
--values values.yml

kubectl --namespace=jupyterhub port-forward service/proxy-public 9080:http

helm uninstall -n jupyterhub jupyterhub

helm repo remove jupyterhub

kubectl delete namespace jupyterhub
