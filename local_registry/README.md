(podman)
sudo vim /etc/containers/registries.conf.d/myregistry.conf 
>>>
[[registry]]
location = "localhost:5000"
insecure = true
>>>



podman run -d -p 5000:5000 --restart=always --name registry registry:2

curl http://localhost:5000/v2/

podman build -t <repo>:<tag> -f s10_jupyter_dockerfile

podman tag <repo>:<tag> localhost:5000/<repo>:<tag>

podman push localhost:5000/<repo>:<tag>

curl localhost:5000/v2/_catalog


export tag=1.0.0
export repo=custom-image

docker build -t ${repo}:${tag} . \
&& docker tag ${repo}:${tag} localhost:5000/${repo}:${tag} \
&& docker push localhost:5000/${repo}:${tag}

