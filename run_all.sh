#!/bin/bash

underlyings=( "HFCL" "DIXON")
dates=("20250519" "20250520" "20250521" "20250522" "20250523")

for u in "${underlyings[@]}"; do
    for d in "${dates[@]}"; do
        echo "Running: $u on $d"
        python script.py "$u" "$d"
    done
done
