#!/bin/bash
num_operations=100000

run_different_operations_test() {
    percentages=(
        "75 10 10 5"
        "95 2 2 1"
        "90 6 4 1"
        "25 25 25 25"
    )

    for percentage in "${percentages[@]}"; do
        read read_ insert_ update_ delete_ <<< $percentage
        python3 tester.py -n $num_operations --insert $insert_ --update $update_ --delete $delete_ --read $read_
        python3 tester.py -n $num_operations --insert $insert_ --update $update_ --delete $delete_ --read $read_ --nuke
    done
}

run_different_number_threads_test() {
    number_threads="1 2 3 4 5 6 7 8 9 10 11 12"

    for number_thread in $number_threads; do
        python3 tester.py -n $num_operations -c $number_thread
        python3 tester.py -n $num_operations -c $number_thread --nuke
    done
}

run_latency_test() {
    python3 tester.py -n $num_operations --latency
    python3 tester.py -n $num_operations --latency --nuke
}

run_latency_test
run_different_number_threads_test
run_different_operations_test