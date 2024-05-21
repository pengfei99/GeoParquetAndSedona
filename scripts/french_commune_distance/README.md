# how to run this script

## create a osrm backend

```shell

# use Multi-Level Dijkstra (MLD)
sudo docker run -d --name osrm_back_mld -t -i -p 5000:5000 -v "/home/public/data:/data" ghcr.io/project-osrm/osrm-backend osrm-routed --algorithm mld /data/france-latest.osrm 

# use Contraction Hierarchies (CH)
sudo docker run -d --name osrm_back_ch -t -i -p 5000:5000 -v "/home/public/data:/data" ghcr.io/project-osrm/osrm-backend osrm-routed --algorithm ch /data/france-latest.osrm 

# clean container with their status
# below command remove all exited container
docker rm -v $(docker ps --filter status=exited -q)
```

```shell
spark-submit --name "my app" --master local[*] --conf spark.ui.enabled=false --conf spark.driver.memeory=6g  example.py
```

