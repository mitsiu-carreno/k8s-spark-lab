# Project
In this repo will be created a small cluster running on kubernetes that supports:
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
minikube start --network=bridge --memory 8g --cpus 8        

## Helm
version.BuildInfo{Version:"3.15.4", GitCommit:"fa9efb07d9d8debbb4306d72af76a383895aa8c4", GitTreeState:"clean", GoVersion:"go1.21.12"}

# Charts
## Spark
| Property | Value |
|---------------|--------|
| URL | https://charts.bitnami.com/bitnami |
| Chart | /spark |
| Chart version | 9.2.12 |
| App version   | 3.5.3  |

## Minio
| Property | Value |
|---------------|--------|
| URL | https://charts.min.io/ |
| Chart | /minio |
| Chart version | 5.3.0 |
| App version   | RELEASE.2024-04-18T19-09-19Z  |

## Jupyterhub      
| Property | Value |
|---------------|--------|
| URL | https://hub.jupyter.org/helm-chart/ |
| Chart | /jupyterhub |
| Chart version | 3.3.8 |
| App version   | 4.1.6  |

# Project structure
There's a dedicated folder for each chart (plus a quick guide into setting up a **local registry**) in each chart folder you will find a README.md laying out all the available commands for setting up, a `values.yml` file when specific tweaking is required/recommended and in some cases other complementary files like test-scripts, all files purpouses and usage are explained in the corresponding README.md.

There's also a `how-to-cluster.md` file in which it's explained the recommended way of using all helm charts to create a spark-cluster using both minio as distributed object storage and jupyterhub with Pyspark as application driver.
