#!/bin/bash


# for partitions in 256 512 1024 2048 4096 8192
# do
#     ./submit.sh Benchmark5 "$partitions false"
# done

# for partitions in 256 512 1024 2048 4096 8192
# do
#     ./submit.sh Benchmark5 "$partitions true"
# done

for partitions in 128 256 512 1024 2048 4096 8192
do
    ./submit.sh Benchmark6 "$partitions false 1"
done

for partitions in 128 256 512 1024 2048 4096 8192
do
    ./submit.sh Benchmark6 "$partitions true 1"
done


# for partitions in 65536
# do
#     ./submit.sh Benchmark3 "$partitions true"
# done

# for partitions in 65536
# do
#     ./submit.sh Benchmark3 "$partitions false"
# done
