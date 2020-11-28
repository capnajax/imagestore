#!/bin/bash

rsync -avz -e ssh --stats --progress . \
  --exclude "build" \
  --exclude ".git" \
  --exclude "node_modules" \
  k8s.moon:devops/imagestore

ssh k8s.moon \
  'cd devops/imagestore ;\
   sudo docker build -t imagestore . ;\
   sudo docker image tag imagestore registry.moon:80/imagestore ;\
   sudo docker push registry.moon:80/imagestore ;\
   kubectl -n moon rollout restart deployment imagestore'
