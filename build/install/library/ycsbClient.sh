# # Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# # http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

# #/bin/bash
# java -Djava.security.properties=./config/java.security -Dlogback.configurationFile="./config/logback.xml" -cp ./lib/*:./bin/ com.yahoo.ycsb.Client -threads 30 -P config/workloads/workloada -p measurementtype=timeseries -p timeseries.granularity=1000 -db bftsmart.demo.ycsb.YCSBClient -s
# java -Djava.security.properties=./config/java.security \
#      -Dlogback.configurationFile="./config/logback.xml" \
#      -cp ./lib/*:./bin/ \
#      com.yahoo.ycsb.Client \
#      -threads ${THREADS} \
#      -P config/workloads/workloada \
#      -p measurementtype=timeseries \
#      -p timeseries.granularity=1000 \
#      -db bftsmart.demo.ycsb.YCSBClient \
#      -s > "${OUTPUT_FILE}" 2>&1

#!/bin/bash
# YCSB Client: ./ycsbClient.sh [threads] [run_id]
# Usage: ./ycsbClient.sh 64 geo_utah_paris_v1

THREADS=${1:-30}           # Default: 30 threads
RUN_ID=${2:-benchmark}     # Default: "benchmark"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="ycsb_results_${THREADS}threads_${RUN_ID}_${TIMESTAMP}.txt"

echo "🧪 YCSB: ${THREADS} threads, run=${RUN_ID}"
echo "📄 Output → ${OUTPUT_FILE}"

java -Djava.security.properties=./config/java.security \
     -Dlogback.configurationFile="./config/logback.xml" \
     -cp ./lib/*:./bin/ \
     com.yahoo.ycsb.Client \
     -threads ${THREADS} \
     -P config/workloads/workloada \
     -p measurementtype=timeseries \
     -p timeseries.granularity=1000 \
     -db bftsmart.demo.ycsb.YCSBClient \
     -s > "${OUTPUT_FILE}" 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Success! Results: ${OUTPUT_FILE}"
    echo "📊 Key metrics:"
    grep -E "(OVERALL|READ|UPDATE).*Throughput|Latency|ops/sec" "${OUTPUT_FILE}" || true
else
    echo "❌ YCSB failed! Check ${OUTPUT_FILE}"
fi
