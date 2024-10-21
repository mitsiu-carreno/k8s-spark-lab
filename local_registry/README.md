Just a little side-note in **podman** you can specify insecure registries just be carefull when setting this up, for the cluster is not necesary (running in minikube) but preserved for educational purposes:
```
cat >> /etc/containers/registries.conf.d/myregistry.conf << EOF
[[registry]]
location = "localhost:5000"
insecure = true
EOF
```
This will allow pulling images like localhost:5000/custom-image over http

# Registry setup
We will start by running a container with registry
```
podman run -d -p 5000:5000 --restart=always --name registry registry:2
```

We can confirm that the registry is running by trying to access /v2/ we should get a **{}** as response
```
curl http://localhost:5000/v2/
```

# Pushing image into our registry
First we have to build our image based on a Dockerfile
```
podman build -t <repo>:<tag> -f s10_jupyter_dockerfile
```

Then we tag it into our local registry
```
podman tag <repo>:<tag> localhost:5000/<repo>:<tag>
```

Finally we can push it
```
podman push localhost:5000/<repo>:<tag>
```

To confirm our image availability we can try to access /v2/_catalog our image should be listed
```
curl localhost:5000/v2/_catalog
```

In order to avoid typos in either the tag or the repo you can instead execute the following commands
```
export tag=1.0.0
export repo=custom-image

docker build -t ${repo}:${tag} . \
&& docker tag ${repo}:${tag} localhost:5000/${repo}:${tag} \
&& docker push localhost:5000/${repo}:${tag}
```

Now you can `podman pull localhost:5000/<repo>:<tag>` when needed (as in jupyterhub/values)
