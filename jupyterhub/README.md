helm repo add jupyterhub https://hub.jupyter.org/helm-chart/

helm repo update

helm upgrade \
--install jupyterhub jupyterhub/jupyterhub \
--namespace jupyterhub \
--create-namespace \
--cleanup-on-fail \
--set prePuller.hook.enabled=false 

helm upgrade \
--install jupyterhub jupyterhub/jupyterhub \
--namespace jupyterhub \
--create-namespace  \
--cleanup-on-fail \
--values values.yml

kubectl --namespace=jupyterhub port-forward service/proxy-public 9080:http

# If jupyter is driver
kubectl --namespace=jupyterhub port-forward jupyter-mit 4040:4040

helm uninstall -n jupyterhub jupyterhub

helm repo remove jupyterhub

kubectl delete namespace jupyterhub
