#!/bin/bash

# Python executable
python=".venv/bin/python"

ASSEMBLER="code/assembler/main.py"
PROFILER="code/profiler/main.py"
SAMPLER="code/sampler/main.py"


# folder="amazon/dataset1"
# server="amazon"
# $python $ASSEMBLER --input="$folder/data"  \
#                    --provider="$server"    \
#                    --tests="$folder/tests" \
#                    --events="$folder/events"

# folder="amazon/dataset1"
# server="amazon"
# $python $PROFILER  --input="$folder/events"  \
#                    --provider="$server"      \

# folder="dazn/dataset2"
# server="dazn"
# rates=("1500kbits" "3000kbits" "4500kbits" "6000kbits" "7500kbits" "50000kbits")
# for rate in "${rates[@]}"; do
#     $python $ASSEMBLER --input="$folder/$rate/data" --provider="$server" --tests="$folder/$rate/tests" --events="$folder/$rate/events"
# done

# folder="dazn/dataset2"
# server="dazn"
# rates=("1500kbits" "3000kbits" "4500kbits" "6000kbits" "7500kbits" "50000kbits")
# for rate in "${rates[@]}"; do
#     for size in $(seq 5000 1000 120000); do
#         $python $SAMPLER                         \
#             --window=$size                       \
#             --input="$folder/$rate/events"       \
#             --provider="$server"                 \
#             --samples="$folder/$rate/samples"    #&
#     done
#     # wait
# done



# # Copy dataset1
# src="$HOME/Desktop/thesis-code/dazn/dataset1"
# dst="$HOME/Desktop/thesis-blog/dazn/dataset1"
# mkdir -p "$dst/tests"
# for item in $(ls "$src/tests" | sort -V | head -n 10); do
#     cp -r "$src/tests/$item" "$dst/tests/"
# done

# Copy dataset2
# main="$HOME/Desktop/thesis-code/dazn/dataset2"
# dst="$HOME/Desktop/thesis-blog/dazn/dataset2"

# rates=("1500kbits" "3000kbits" "4500kbits" "6000kbits" "7500kbits" "50000kbits")

# for rate in "${rates[@]}"; do
#     src="$main/$rate"
#     mkdir -p "$dst/$rate/tests"
#     mkdir -p "$dst/$rate/samples"
#     for item in $(ls "$src/tests" | sort -V | head -n 20); do
#         cp -r "$src/tests/$item" "$dst/$rate/tests/"
#     done
#     for item in $(ls "$src/samples" | sort -V | head -n 20); do
#         cp -r "$src/tests/$item" "$dst/$rate/tests/"
#     done
# done



folder="dazn/dataset2"
server="dazn"
$python $ASSEMBLER --input="$folder/data"  \
                   --provider="$server"    \
                   --tests="$folder/tests" \
                   --events="$folder/events"

folder="dazn/dataset3"
server="dazn"
$python $ASSEMBLER --input="$folder/data"  \
                   --provider="$server"    \
                   --tests="$folder/tests" \
                   --events="$folder/events"

folder="dazn/dataset2"
server="dazn"
rates=("1500kbits" "3000kbits" "4500kbits" "6000kbits" "7500kbits" "50000kbits")
window=10000
for rate in "${rates[@]}"; do
    $python $SAMPLER                                 \
                --window=$window                     \
                --input="$folder/$rate/events"       \
                --provider="$server"                 \
                --samples="$folder/$rate/samples"    
done

folder="dazn/dataset3"
server="dazn"
window=10000
$python $SAMPLER                                 \
            --window=$window                     \
            --input="$folder/$rate/events"       \
            --provider="$server"                 \
            --samples="$folder/$rate/samples"    