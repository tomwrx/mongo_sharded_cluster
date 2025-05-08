### A guide of creating Mongo cluster and running sharded MongoDB with Docker Compose. It is assumed that Docker Desktop is already installed. A folder structure should be the same as in project.

Examples tested in PowerShell

1. Check if docker-compose is installed

```bash
docker-compose --version
```
should return something similar to:
 ```bash
Docker Compose version v2.34.0-desktop.1
```

2. Enable WSL2 and Virtual Machine Platform

```bash
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
```

Then restart your computer.

3. Testing Docker

```bash
docker --version  
docker-compose --version  
docker run hello-world
```  

should return something like:
Docker version 28.0.4, build b8034c0  
Docker Compose version v2.34.0-desktop.1  
downloads hello-world image

4.  Start the cluster via docker compose from a folder where docker-compose.yml is located

```bash
docker-compose up -d
```

5. Initiate Replica Sets:
```bash
docker exec -it configsvr1 mongosh --port 27019
rs.initiate({ _id: "configReplSet", configsvr: true, members: [{ _id: 0, host: "configsvr1:27019" }] })
exit
```
```bash
docker exec -it shard1 mongosh --port 27018
rs.initiate({ _id: "shard1ReplSet", members: [{ _id: 0, host: "shard1:27018" }] })
exit
```
```bash
docker exec -it shard2 mongosh --port 27020
rs.initiate({ _id: "shard2ReplSet", members: [{ _id: 0, host: "shard2:27020" }] })
exit
```

6. Connect to mongos and add shards:
```bash
docker exec -it mongos mongosh --port 27017
sh.addShard("shard1ReplSet/shard1:27018")
sh.addShard("shard2ReplSet/shard2:27020")
```

7. Enabling sharding on vessel mmsi with hashing because of high cardinality

```bash
docker exec -it mongos mongosh --port 27017
sh.enableSharding("ais_tracking")
sh.shardCollection("ais_tracking.positions", { mmsi: "hashed" })
````

8. Processing and inserting data to MongoDB in parallel

run in powershell 

```bash
python parallel_processing.py
```
or run a file simply in visual studio code ;)

9. Count rows of inserted data

```bash
docker cp count_docs.js mongos:/tmp/count_docs.js
docker exec -it mongos mongosh /tmp/count_docs.js
```

should return somnething like:
```Total documents in positions: 5959288```

10. Important notes

- You must not have the same database on multiple shards before adding them.
- mongos must be bound to all IPs using `--bind_ip_all`.
- Shards must be initiated **before** adding to mongos.


### An error simulation guidelines

1. Connecto to ```mongos```
```bash
docker exec -it mongos mongosh
use ais_tracking
db.positions.countDocuments({ mmsi: { $exists: true } })
```

2. Failure simulation

```bash
docker stop shard2
```

3. We need to run the same query (step 1) again via mongos. It should still return results, assuming shard1 has part of the data.

4. Restore the shard
```bash
docker start shard2
```
