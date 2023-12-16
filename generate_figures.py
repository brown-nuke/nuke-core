import os
import csv
import matplotlib.pyplot as plt
import numpy as np

def main():
    generate_latency_threads()
    generate_operation_distribution()
    generate_throughput_threads()

def generate_operation_distribution():
    distributions = ['[75 10 10 5]', '[95 2 2 1]', '[90 6 4 1]', '[25 25 25 25]']
    latencies_with_nuke = {distribution: [] for distribution in distributions}
    latencies_without_nuke = {distribution: [] for distribution in distributions}

    with open('distribution_results.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            distribution = row['operation distribution']
            throughput = float(row['throughput'])
            with_nuke = row['with nuke'] == 'yes'

            if with_nuke:
                latencies_with_nuke[distribution].append(throughput)
            else:
                latencies_without_nuke[distribution].append(throughput)

    # Calculate the average latencies
    avg_latencies_with_nuke = [np.mean(latencies_with_nuke[distribution]) for distribution in distributions]
    avg_latencies_without_nuke = [np.mean(latencies_without_nuke[distribution]) for distribution in distributions]

    # Generate the plot
    x = np.arange(len(distributions))
    width = 0.35

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, avg_latencies_with_nuke, width, label='Nuke', color='#F08533')
    rects2 = ax.bar(x + width/2, avg_latencies_without_nuke, width, label='Baseline', color='#3F74AA')

    ax.set_ylabel('Throughput (operations/second)')
    ax.set_xlabel('Distribution of database operations [read, insert, update, delete]')
    ax.set_title('Throughput for different operation distributions')
    ax.set_xticks(x)
    ax.set_xticklabels(distributions)
    ax.legend()

    fig.tight_layout()

    plt.savefig("distribution_result.png", format="png", bbox_inches="tight", dpi=1200)

def generate_latency_threads():
    operations = ['insert', 'update', 'delete', 'read']
    latencies_with_nuke = {operation: [] for operation in operations}
    latencies_without_nuke = {operation: [] for operation in operations}

    with open('latency_results.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            operation = row['operation']
            latency = float(row['latency'])
            with_nuke = row['with nuke'] == 'yes'

            if with_nuke:
                latencies_with_nuke[operation].append(latency)
            else:
                latencies_without_nuke[operation].append(latency)

    # Calculate the average latencies
    avg_latencies_with_nuke = [np.mean(latencies_with_nuke[operation]) for operation in operations]
    avg_latencies_without_nuke = [np.mean(latencies_without_nuke[operation]) for operation in operations]

    # Generate the plot
    x = np.arange(len(operations))
    width = 0.35

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, avg_latencies_with_nuke, width, label='Nuke', color='#F08533')
    rects2 = ax.bar(x + width/2, avg_latencies_without_nuke, width, label='Baseline', color='#3F74AA')

    ax.set_ylabel('Latency (ms)')
    ax.set_title('Latency by Database Operation')
    ax.set_xticks(x)
    ax.set_xticklabels(operations)
    ax.legend()

    fig.tight_layout()

    plt.savefig("latency_result.png", format="png", bbox_inches="tight", dpi=1200)

# Specify the CSV file path
def generate_throughput_threads():
    csv_file = "final_throughput.csv"

    plt.figure(figsize=(8, 6))

    data = {}

    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            name = row['nuke?']  # Replace 'name' with the actual column name containing name values
            throughput = float(row['throughput'])  # Assuming width is a numerical value
            n_threads = float(row['threads'])    # Assuming time is a numerical value

            # If the name is not in the dictionary, create a new list
            if name not in data:
                data[name] = {'threads': [], 'throughput': []}

            # Append width and time to the respective lists for the name value
            if n_threads in data[name]['threads']:
                data[name]['throughput'][data[name]['threads'].index(n_threads)] += throughput
            else:
                data[name]['threads'].append(n_threads)
                data[name]['throughput'].append(throughput)

    colors = ["#F08533", "#3F74AA"]
    markers = ["o", "+"]
    for name, values in data.items():
        plt.plot(values['threads'], values['throughput'], color=colors.pop(0), marker=markers.pop(0), label=name)

    plt.title('Throughput vs. Number of Threads')
    plt.ylabel('Throughput (operations/second)')
    plt.xlabel('Number of Threads')
    plt.legend()

    plt.ylim(0, None)

    plt.savefig("throughput_result.png", format="png", bbox_inches="tight", dpi=1200)

if __name__ == "__main__":
    main()