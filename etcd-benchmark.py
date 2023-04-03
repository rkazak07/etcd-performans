import time
import random
import string
import threading
import etcd3
from concurrent import futures
import argparse


class Benchmark:
    def __init__(self, num_threads, num_ops, key_size, value_size):
        self.num_threads = num_threads
        self.num_ops = num_ops
        self.key_size = key_size
        self.value_size = value_size
        self.results = []

    def generate_random_string(self, size):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(size))

    def put_task(self, etcd, key, value):
        try:
            start_time = time.monotonic()
            etcd.put(key, value)
            end_time = time.monotonic()
            latency = end_time - start_time
            self.results.append(latency)
        except Exception as e:
            print('Error during put: {}'.format(str(e)))

    def run_benchmark(self):
        etcd = etcd3.client()
        keys = [self.generate_random_string(self.key_size) for _ in range(self.num_ops)]
        values = [self.generate_random_string(self.value_size) for _ in range(self.num_ops)]
        with futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            for i in range(self.num_ops):
                executor.submit(self.put_task, etcd, keys[i], values[i])
        executor.shutdown(wait=True)
        return self.results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_threads', type=int, default=10)
    parser.add_argument('--num_ops', type=int, default=10000)
    parser.add_argument('--key_size', type=int, default=8)
    parser.add_argument('--value_size', type=int, default=128)
    args = parser.parse_args()

    benchmark = Benchmark(args.num_threads, args.num_ops, args.key_size, args.value_size)

    start_time = time.monotonic()
    results = benchmark.run_benchmark()
    end_time = time.monotonic()

    total_latency = sum(results)
    num_ops_completed = len(results)
    average_latency = total_latency / num_ops_completed

    print('Number of operations: {}'.format(num_ops_completed))
    print('Total time: {} seconds'.format(end_time - start_time))
    print('Average latency: {} seconds'.format(average_latency))
