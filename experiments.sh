/*
Copyright 2016 katsogr

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

#!/bin/bash

# echo "Distributed scheduling false"
# for partitions in 512 1024 2048 4096 8192 16384 32768
# do
#     ./submit.sh Benchmark5 "$partitions false"
# done

# echo "Distributed scheduling true"
# for partitions in 512 1024 2048 4096 8192 16384 32768
# do
#     ./submit.sh Benchmark5 "$partitions true 4"
# done

echo "Distributed scheduling false"
for partitions in 512 1024 #2048 4096 8192 16384 32768
do
    ./submit.sh Filter "$partitions false"
done

echo "Distributed scheduling true"
for partitions in 512 1024  #2048 4096 8192 16384 32768
do
    ./submit.sh Filter "$partitions true 4"
done



# echo "Distributed scheduling false"
# for partitions in 256 512 1024 2048 4096 8192
# do
#     ./submit.sh Benchmark6 "$partitions false 0"
# done

# echo "Distributed scheduling true"
# for partitions in 256 512 1024 2048 4096 8192
# do
#     ./submit.sh Benchmark6 "$partitions true 0"
# done

# for scheduler in 1 2 4 8 16 32
# do
#     ./submit.sh Benchmark5 "$partitions 8192 true $scheduler"
# done



# for partitions in 128 256 512 1024 2048 4096 8192
# do
#     ./submit.sh Benchmark6 "$partitions false 1"
# done

# for partitions in 128 256 512 1024 2048 4096 8192
# do
#     ./submit.sh Benchmark6 "$partitions true 1"
# done


# for partitions in 65536
# do
#     ./submit.sh Benchmark3 "$partitions true"
# done

# for partitions in 65536
# do
#     ./submit.sh Benchmark3 "$partitions false"
# done
