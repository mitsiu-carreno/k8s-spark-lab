# Project
This is the result of severe thesis writing avoidance, but formally is part of the metodology setup, in this repo will be created a small cluster running on kubernetes that supports:
- Spark worker-master architecture
- Minio as distributed file system
- Jupyterhub as pyspark driver

## Disclaimer
This is in no way production ready, several secrets are hardcoded, instead is just for educational purposes.

# Requirements

## Kubernetes 
Client Version: v1.29.9
Kustomize Version: v5.0.4-0.20230601165947-6ce0bf390ce3
Server Version: v1.28.3

## Minihube
minikube version: v1.32.0
commit: 8220a6eb95f0a4d75f7f2d7b14cef975f050512d

## Helm
version.BuildInfo{Version:"3.15.4", GitCommit:"fa9efb07d9d8debbb4306d72af76a383895aa8c4", GitTreeState:"clean", GoVersion:"go1.21.12"}

# Charts
## Spark
Chart version: spark-9.2.12
App version: 3.5.3

## Minio
Chart version: minio-5.3.0
App version: RELEASE.2024-04-18T19-09-19Z

## Jupyterhub
Chart version: jupyterhub-3.3.8
App version: 4.1.6 
