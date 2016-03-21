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
