#!/usr/bin/env python3
"""
Test script to verify ML API performance improvements.
"""

import asyncio
import aiohttp
import time
import json
from typing import List, Dict, Any

async def test_single_prediction(session: aiohttp.ClientSession, flow_data: Dict[str, Any]) -> Dict[str, Any]:
    """Test single flow prediction."""
    start_time = time.time()
    
    try:
        async with session.post(
            "http://localhost:8000/predict",
            json={"flow": flow_data},
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            if response.status == 200:
                result = await response.json()
                processing_time = (time.time() - start_time) * 1000
                return {
                    "success": True,
                    "processing_time_ms": processing_time,
                    "prediction": result.get("prediction"),
                    "confidence": result.get("confidence")
                }
            else:
                error_text = await response.text()
                return {
                    "success": False,
                    "error": f"HTTP {response.status}: {error_text}",
                    "processing_time_ms": (time.time() - start_time) * 1000
                }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "processing_time_ms": (time.time() - start_time) * 1000
        }

async def test_batch_prediction(session: aiohttp.ClientSession, flows_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Test batch prediction."""
    start_time = time.time()
    
    try:
        async with session.post(
            "http://localhost:8000/predict_batch",
            json={"flows": flows_data},
            timeout=aiohttp.ClientTimeout(total=60)
        ) as response:
            if response.status == 200:
                result = await response.json()
                processing_time = (time.time() - start_time) * 1000
                return {
                    "success": True,
                    "processing_time_ms": processing_time,
                    "total_flows": result.get("total_flows"),
                    "successful_predictions": result.get("successful_predictions"),
                    "batch_time_ms": result.get("batch_time_ms"),
                    "avg_time_per_flow_ms": result.get("avg_time_per_flow_ms")
                }
            else:
                error_text = await response.text()
                return {
                    "success": False,
                    "error": f"HTTP {response.status}: {error_text}",
                    "processing_time_ms": (time.time() - start_time) * 1000
                }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "processing_time_ms": (time.time() - start_time) * 1000
        }

def create_sample_flow() -> Dict[str, Any]:
    """Create a sample flow for testing."""
    return {
        "flow_id": "test_flow_1",
        "timestamp": "2025-06-23 10:30:00",
        "src_ip": "192.168.1.100",
        "src_port": 12345,
        "dst_ip": "192.168.1.200",
        "dst_port": 80,
        "protocol": "TCP",
        "total_header_bytes": 100,
        "max_header_bytes": 50,
        "min_header_bytes": 20,
        "mean_header_bytes": 35,
        "median_header_bytes": 35,
        "mode_header_bytes": 20,
        "fwd_total_header_bytes": 60,
        "fwd_max_header_bytes": 30,
        "fwd_min_header_bytes": 15,
        "fwd_mean_header_bytes": 25,
        "fwd_median_header_bytes": 25,
        "fwd_mode_header_bytes": 15,
        "fwd_segment_size_mean": 1000,
        "fwd_segment_size_max": 1500,
        "fwd_segment_size_min": 500,
        "fwd_segment_size_std": 200,
        "fwd_segment_size_variance": 40000,
        "fwd_segment_size_median": 1000,
        "fwd_segment_size_skewness": 0.1,
        "fwd_segment_size_cov": 0.2,
        "fwd_segment_size_mode": 1000,
        "bwd_segment_size_mean": 800,
        "bwd_segment_size_max": 1200,
        "bwd_segment_size_min": 400,
        "bwd_segment_size_std": 150,
        "bwd_segment_size_variance": 22500,
        "bwd_segment_size_median": 800,
        "bwd_segment_size_skewness": 0.05,
        "bwd_segment_size_cov": 0.19,
        "bwd_segment_size_mode": 800,
        "segment_size_mean": 900,
        "segment_size_max": 1350,
        "segment_size_min": 450,
        "segment_size_std": 175,
        "segment_size_variance": 30625,
        "segment_size_median": 900,
        "segment_size_skewness": 0.08,
        "segment_size_cov": 0.19,
        "segment_size_mode": 900,
        "fwd_init_win_bytes": 65535,
        "bwd_init_win_bytes": 65535,
        "packets_rate": 10.5,
        "bwd_packets_rate": 5.2,
        "fwd_packets_rate": 5.3,
        "rst_flag_counts": 0,
        "bwd_rst_flag_counts": 0,
        "psh_flag_percentage_in_total": 0.3,
        "rst_flag_percentage_in_total": 0.0,
        "fwd_psh_flag_percentage_in_total": 0.15,
        "fwd_syn_flag_percentage_in_fwd_packets": 0.1,
        "bwd_psh_flag_percentage_in_bwd_packets": 0.15,
        "bwd_rst_flag_percentage_in_bwd_packets": 0.0,
        "bwd_packets_IAT_mean": 0.1,
        "bwd_packets_IAT_std": 0.05,
        "bwd_packets_IAT_max": 0.2,
        "bwd_packets_IAT_min": 0.01,
        "bwd_packets_IAT_total": 1.0,
        "bwd_packets_IAT_median": 0.1,
        "bwd_packets_IAT_mode": 0.1,
        "handshake_duration": 0.5,
        "handshake_state": 3,
        "mean_bwd_packets_delta_time": 0.1,
        "median_bwd_packets_delta_time": 0.1,
        "skewness_packets_delta_len": 0.1,
        "mode_fwd_packets_delta_len": 1000,
        "label": "Benign"
    }

async def main():
    """Main test function."""
    print("Testing ML API Performance...")
    print("=" * 50)
    
    # Create sample flows
    sample_flow = create_sample_flow()
    sample_flows = [create_sample_flow() for _ in range(50)]  # 50 flows for batch test
    
    async with aiohttp.ClientSession() as session:
        # Test 1: Single prediction
        print("\n1. Testing Single Flow Prediction...")
        single_result = await test_single_prediction(session, sample_flow)
        
        if single_result["success"]:
            print(f"✅ Single prediction successful!")
            print(f"   Processing time: {single_result['processing_time_ms']:.2f} ms")
            print(f"   Prediction: {single_result['prediction']}")
            print(f"   Confidence: {single_result['confidence']:.4f}")
        else:
            print(f"❌ Single prediction failed: {single_result['error']}")
        
        # Test 2: Batch prediction
        print("\n2. Testing Batch Prediction (50 flows)...")
        batch_result = await test_batch_prediction(session, sample_flows)
        
        if batch_result["success"]:
            print(f"✅ Batch prediction successful!")
            print(f"   Total processing time: {batch_result['processing_time_ms']:.2f} ms")
            print(f"   Batch time (API): {batch_result['batch_time_ms']:.2f} ms")
            print(f"   Avg time per flow: {batch_result['avg_time_per_flow_ms']:.2f} ms")
            print(f"   Successful predictions: {batch_result['successful_predictions']}/{batch_result['total_flows']}")
        else:
            print(f"❌ Batch prediction failed: {batch_result['error']}")
        
        # Test 3: Multiple single predictions (for comparison)
        print("\n3. Testing Multiple Single Predictions (10 flows)...")
        single_times = []
        for i in range(10):
            result = await test_single_prediction(session, sample_flows[i])
            if result["success"]:
                single_times.append(result["processing_time_ms"])
        
        if single_times:
            avg_single_time = sum(single_times) / len(single_times)
            total_single_time = sum(single_times)
            print(f"✅ Multiple single predictions completed!")
            print(f"   Average time per flow: {avg_single_time:.2f} ms")
            print(f"   Total time for 10 flows: {total_single_time:.2f} ms")
            
            # Compare with batch
            if batch_result["success"]:
                batch_avg = batch_result["avg_time_per_flow_ms"]
                improvement = ((avg_single_time - batch_avg) / avg_single_time) * 100
                print(f"   Batch improvement: {improvement:.1f}% faster")
    
    print("\n" + "=" * 50)
    print("Performance test completed!")

if __name__ == "__main__":
    asyncio.run(main()) 