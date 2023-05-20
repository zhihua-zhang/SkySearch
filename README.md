<p><img src="https://github.com/zhihua-zhang/SkySearch/blob/main/asset/crawler.png" width = "300" height = "200" alt="crawler" title="Crawler Workflow"/></p>
<p><img src="https://github.com/zhihua-zhang/SkySearch/blob/main/asset/search.png" alt="search" title="Search Workflow"/></p>
<p><img src="https://github.com/zhihua-zhang/SkySearch/blob/main/asset/suggest.png" alt="suggest" title="Search Suggestion"/></p>
<p><img src="https://github.com/zhihua-zhang/SkySearch/blob/main/asset/spellcheck.png" alt="spellcheck" title="Search Spellcheck"/></p>
<p><img src="https://github.com/zhihua-zhang/SkySearch/blob/main/asset/example_eagles.png" alt="example_eagles" title="Search 'eagles philly'"/></p>
<p><img src="https://github.com/zhihua-zhang/SkySearch/blob/main/asset/example_fed.png" alt="example_fed" title="Search 'fed reserve interest rate'"/></p>
<p><img src="https://github.com/zhihua-zhang/SkySearch/blob/main/asset/example_sf.png" alt="example_sf" title="Search 'visiting san francisco'"/></p>


To download Gson library jar: https://github.com/google/gson


Compile:

1. KVS:

- javac -d bin/ --source-path src src/cis5550/kvs/Master.java 

- javac -d bin/ --source-path src src/cis5550/kvs/KVSClient.java

- javac -d bin/ --source-path src src/cis5550/kvs/Worker.java


2. FLAME:

- javac -d bin/ --source-path src src/cis5550/flame/Master.java

- javac -d bin/ --source-path src src/cis5550/flame/Worker.java

- javac -d bin/ --source-path src src/cis5550/flame/FlameSubmit.java


3. JOBS:

- javac -d bin/ --source-path src src/cis5550/jobs/Crawler.java

- jar cf src/crawler.jar bin/cis5550/jobs/Crawler.class

- javac -d bin/ --source-path src src/cis5550/jobs/Indexer.java

- jar cf src/indexer.jar bin/cis5550/jobs/Indexer.class

- javac -d bin/ --source-path src src/cis5550/jobs/PageRank.java

- jar cf src/pagerank.jar bin/cis5550/jobs/Pagerank.class

- javac -d bin/ --source-path src src/cis5550/jobs/TitleExtract.java

- jar cf src/titleExtract.jar bin/cis5550/jobs/TitleExtract.class


4. FRONTEND:

- javac -d bin/ -cp lib/gson-2.10.1.jar:src src/cis5550/frontend/LoadBalancer.java

- javac -d bin/ -cp lib/gson-2.10.1.jar:src src/cis5550/frontend/Server.java


5. RANKER:

- javac -d bin/ -cp lib/gson-2.10.1.jar:src src/cis5550/ranker/Ranker.java


Run:

1. KVS:

- java -classpath bin cis5550/kvs/Master 8000

- java -classpath bin cis5550.kvs.Worker 8001 <dir_for_worker1_data> <KVMasterIP:Port>

- java -classpath bin cis5550.kvs.Worker 8002 <dir_for_worker2_data> <KVMasterIP:Port>

- java -classpath bin cis5550.kvs.Worker 8003 <dir_for_worker3_data> <KVMasterIP:Port>

- java -classpath bin cis5550.kvs.Worker 8004 <dir_for_worker4_data> <KVMasterIP:Port>


2. FLAME:

- java -classpath bin cis5550.flame.Master 9000 <KVMasterIP:Port>

- java -classpath bin cis5550.flame.Worker 9001 <FlameMasterIP:Port>

- java -classpath bin cis5550.flame.Worker 9002 <FlameMasterIP:Port>

- java -classpath bin cis5550.flame.Worker 9003 <FlameMasterIP:Port>

- java -classpath bin cis5550.flame.Worker 9004 <FlameMasterIP:Port>


3. JOBS:

- java -classpath bin cis5550.flame.FlameSubmit <FlameMasterIP:Port> src/crawler.jar cis5550.jobs.Crawler https://en.wikipedia.org/wiki/University_of_Pennsylvania

- java -classpath bin cis5550.flame.FlameSubmit <FlameMasterIP:Port> src/indexer.jar cis5550.jobs.Indexer

- java -classpath bin cis5550.flame.FlameSubmit <FlameMasterIP:Port> src/pagerank.jar - cis5550.jobs.PageRank

- java -classpath bin cis5550.flame.FlameSubmit <FlameMasterIP:Port> src/titleExtract.jar cis5550.jobs.TitleExtract


4. FRONTEND:

- java -cp lib/gson-2.10.1.jar:bin cis5550.frontend.LoadBalancer 5000

- java -cp lib/gson-2.10.1.jar:bin cis5550.frontend.Server 8080 <LoadBalancerIP:Port> <KVMasterIP:Port>

- java -cp lib/gson-2.10.1.jar:bin cis5550.frontend.Server 8081 <LoadBalancerIP:Port> <KVMasterIP:Port>

5. RANKER:

- java -cp lib/gson-2.10.1.jar:bin cis5550.ranker.Ranker 8888 <KVMasterIP:Port>

6. WEB:

- Enter http://<LoadBalancerIP>:5000/ or http://skysearch.cis5550.net:5000/ to start
