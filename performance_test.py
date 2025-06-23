#!/usr/bin/env python3
"""
Performance Test Script for DDoS Detection System
Compares original vs optimized performance
"""

import time
import asyncio
import aiohttp
import requests
import json
import pandas as pd
from typing import List, Dict, Any
import statistics
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceTester:
    def __init__(self):
        self.original_api_url = "http://localhost:8000/predict"  # Original API
        self.optimized_api_url = "http://localhost:8000/predict"  # Optimized API
        self.batch_api_url = "http://localhost:8000/predict_batch"  # Batch API
        
        # Test data
        self.test_flows = self.generate_test_flows(100)
        
    def generate_test_flows(self, count: int) -> List[Dict[str, Any]]:
        """Generate test flow data."""
        flows = []
        for i in range(count):
            flow = {
                "flow_id": f"test_flow_{i}",
                "timestamp": "2025-06-23 10:27:42.176137",
                "src_ip": "192.168.1.100",
                "src_port": 12345 + i,
                "dst_ip": "10.0.0.1",
                "dst_port": 80,
                "protocol": "TCP",
                "total_header_bytes": 428 + i,
                "max_header_bytes": 32,
                "min_header_bytes": 20,
                "mean_header_bytes": 22.5263,
                "median_header_bytes": 20.0,
                "mode_header_bytes": 20.0,
                "fwd_total_header_bytes": 224,
                "fwd_max_header_bytes": 32,
                "fwd_min_header_bytes": 20,
                "fwd_mean_header_bytes": 22.4000,
                "fwd_median_header_bytes": 20.0,
                "fwd_mode_header_bytes": 20.0,
                "fwd_segment_size_mean": 390.6000,
                "fwd_segment_size_max": 1428,
                "fwd_segment_size_min": 20,
                "fwd_segment_size_std": 540.9420,
                "fwd_segment_size_variance": 292618.2400,
                "fwd_segment_size_median": 58.0,
                "fwd_segment_size_skewness": 1.2382,
                "fwd_segment_size_cov": 1.3849,
                "fwd_segment_size_mode": 20.0,
                "bwd_segment_size_mean": 383575.7284,
                "bwd_segment_size_max": 421.0,
                "bwd_segment_size_min": 0.3183,
                "bwd_segment_size_std": 0.9890,
                "bwd_segment_size_variance": 1428.0,
                "bwd_segment_size_median": 503.0526,
                "bwd_segment_size_skewness": 1428,
                "bwd_segment_size_cov": 20,
                "bwd_segment_size_mode": 590.8814,
                "segment_size_mean": 349140.7867,
                "segment_size_max": 84.0,
                "segment_size_min": 0.7483,
                "segment_size_std": 1.1746,
                "segment_size_variance": 20.0,
                "segment_size_median": 65535,
                "segment_size_skewness": 65535,
                "segment_size_cov": 15.048191,
                "segment_size_mode": 7.128091,
                "fwd_init_win_bytes": 7.920101,
                "bwd_init_win_bytes": 0,
                "packets_rate": 0,
                "bwd_packets_rate": 0.315789,
                "fwd_packets_rate": 0.0,
                "rst_flag_counts": 0.157895,
                "bwd_rst_flag_counts": 0.100000,
                "psh_flag_percentage_in_total": 0.333333,
                "rst_flag_percentage_in_total": 0.0,
                "fwd_psh_flag_percentage_in_total": 0.1185,
                "fwd_syn_flag_percentage_in_fwd_packets": 0.1486,
                "bwd_psh_flag_percentage_in_bwd_packets": 0.316125,
                "bwd_rst_flag_percentage_in_bwd_packets": 0.0,
                "bwd_packets_IAT_mean": 0.947816,
                "bwd_packets_IAT_std": 0.0087,
                "bwd_packets_IAT_max": 0.0,
                "bwd_packets_IAT_min": 0.3151,
                "bwd_packets_IAT_total": 3,
                "bwd_packets_IAT_median": 118.4770,
                "bwd_packets_IAT_mode": 8.6570,
                "handshake_duration": -0.5313,
                "handshake_state": -12.0,
                "mean_bwd_packets_delta_time": 0.0,
                "median_bwd_packets_delta_time": 0.0,
                "skewness_packets_delta_len": 0.0,
                "mode_fwd_packets_delta_len": 0.0,
                "label": "Benign"
            }
            flows.append(flow)
        return flows

    async def test_single_requests(self, api_url: str, flows: List[Dict], session: aiohttp.ClientSession) -> List[float]:
        """Test single request performance."""
        response_times = []
        
        for flow in flows:
            try:
                start_time = time.time()
                async with session.post(
                    api_url,
                    json={"flow": flow},
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status == 200:
                        await response.json()
                        response_time = (time.time() - start_time) * 1000
                        response_times.append(response_time)
                    else:
                        logger.error(f"Request failed with status {response.status}")
                        
            except Exception as e:
                logger.error(f"Request error: {e}")
                
        return response_times

    async def test_batch_requests(self, api_url: str, flows: List[Dict], session: aiohttp.ClientSession, batch_size: int = 50) -> List[float]:
        """Test batch request performance."""
        response_times = []
        
        # Split flows into batches
        for i in range(0, len(flows), batch_size):
            batch = flows[i:i + batch_size]
            
            try:
                start_time = time.time()
                async with session.post(
                    api_url,
                    json={"flows": batch},
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        response_time = (time.time() - start_time) * 1000
                        response_times.append(response_time)
                        
                        # Log batch results
                        logger.info(f"Batch {i//batch_size + 1}: {len(batch)} flows in {response_time:.2f}ms "
                                  f"({response_time/len(batch):.2f}ms per flow)")
                    else:
                        logger.error(f"Batch request failed with status {response.status}")
                        
            except Exception as e:
                logger.error(f"Batch request error: {e}")
                
        return response_times

    async def test_concurrent_requests(self, api_url: str, flows: List[Dict], session: aiohttp.ClientSession, concurrency: int = 10) -> List[float]:
        """Test concurrent request performance."""
        response_times = []
        
        async def make_request(flow):
            try:
                start_time = time.time()
                async with session.post(
                    api_url,
                    json={"flow": flow},
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status == 200:
                        await response.json()
                        return (time.time() - start_time) * 1000
                    else:
                        return None
            except Exception as e:
                logger.error(f"Concurrent request error: {e}")
                return None
        
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency)
        
        async def limited_request(flow):
            async with semaphore:
                return await make_request(flow)
        
        # Run concurrent requests
        tasks = [limited_request(flow) for flow in flows]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results
        response_times = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        return response_times

    def print_performance_stats(self, test_name: str, response_times: List[float]):
        """Print performance statistics."""
        if not response_times:
            logger.warning(f"No valid response times for {test_name}")
            return
            
        stats = {
            "count": len(response_times),
            "mean": statistics.mean(response_times),
            "median": statistics.median(response_times),
            "min": min(response_times),
            "max": max(response_times),
            "std": statistics.stdev(response_times) if len(response_times) > 1 else 0,
            "p95": sorted(response_times)[int(len(response_times) * 0.95)] if response_times else 0,
            "p99": sorted(response_times)[int(len(response_times) * 0.99)] if response_times else 0
        }
        
        print(f"\n{'='*60}")
        print(f"PERFORMANCE RESULTS: {test_name}")
        print(f"{'='*60}")
        print(f"Total Requests: {stats['count']}")
        print(f"Mean Response Time: {stats['mean']:.2f}ms")
        print(f"Median Response Time: {stats['median']:.2f}ms")
        print(f"Min Response Time: {stats['min']:.2f}ms")
        print(f"Max Response Time: {stats['max']:.2f}ms")
        print(f"Standard Deviation: {stats['std']:.2f}ms")
        print(f"95th Percentile: {stats['p95']:.2f}ms")
        print(f"99th Percentile: {stats['p99']:.2f}ms")
        print(f"Throughput: {1000/stats['mean']:.2f} requests/second" if stats['mean'] > 0 else "Throughput: N/A")
        
        return stats

    async def run_performance_tests(self):
        """Run all performance tests."""
        print("🚀 Starting Performance Tests")
        print("="*60)
        
        # Create HTTP session
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=100)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            # Test 1: Single requests (sequential)
            print("\n📊 Test 1: Single Requests (Sequential)")
            single_times = await self.test_single_requests(self.optimized_api_url, self.test_flows[:20], session)
            single_stats = self.print_performance_stats("Single Requests", single_times)
            
            # Test 2: Batch requests
            print("\n📊 Test 2: Batch Requests")
            batch_times = await self.test_batch_requests(self.batch_api_url, self.test_flows, session, batch_size=50)
            batch_stats = self.print_performance_stats("Batch Requests", batch_times)
            
            # Test 3: Concurrent requests
            print("\n📊 Test 3: Concurrent Requests")
            concurrent_times = await self.test_concurrent_requests(self.optimized_api_url, self.test_flows[:50], session, concurrency=10)
            concurrent_stats = self.print_performance_stats("Concurrent Requests", concurrent_times)
            
            # Performance comparison
            print(f"\n{'='*60}")
            print("PERFORMANCE COMPARISON")
            print(f"{'='*60}")
            
            if single_stats and batch_stats:
                improvement = ((single_stats['mean'] - batch_stats['mean']) / single_stats['mean']) * 100
                print(f"Batch processing is {improvement:.1f}% faster than single requests")
                print(f"Speedup: {single_stats['mean'] / batch_stats['mean']:.2f}x")
            
            if single_stats and concurrent_stats:
                improvement = ((single_stats['mean'] - concurrent_stats['mean']) / single_stats['mean']) * 100
                print(f"Concurrent processing is {improvement:.1f}% faster than single requests")
                print(f"Speedup: {single_stats['mean'] / concurrent_stats['mean']:.2f}x")
            
            # Recommendations
            print(f"\n{'='*60}")
            print("RECOMMENDATIONS")
            print(f"{'='*60}")
            print("✅ Use batch processing for high-throughput scenarios")
            print("✅ Use concurrent requests for real-time processing")
            print("✅ Monitor response times and adjust batch sizes accordingly")
            print("✅ Consider using the optimized producer for production")

async def main():
    """Main function."""
    tester = PerformanceTester()
    await tester.run_performance_tests()

if __name__ == "__main__":
    asyncio.run(main()) 