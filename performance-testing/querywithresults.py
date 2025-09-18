import requests
import time
import json
import statistics
from typing import List, Dict, Any, Tuple
import random
import csv
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os
from datetime import datetime


def execute_query(
    base_url: str,
    endpoint: str,
    params: Dict[str, Any] = None,
    headers: Dict[str, str] = None,
) -> Tuple[float, int, List[Dict[str, Any]]]:
    """
    Execute a single NGSI-LD query

    Args:
        base_url: Base URL of the NGSI-LD context broker
        endpoint: API endpoint (e.g., 'entities')
        params: Query parameters
        headers: HTTP headers

    Returns:
        Tuple of (request_time, status_code, entities)
    """
    url = f"{base_url}/{endpoint}"

    if headers is None:
        headers = {
            "Accept": "application/ld+json",
            "Content-Type": "application/ld+json",
        }

    start_time = time.time()

    try:
        response = requests.get(url, params=params, headers=headers)
        end_time = time.time()
        request_time = end_time - start_time
        #print("called URL: " + response.url)
        if response.status_code == 200:
            entities = response.json()
            return request_time, response.status_code, entities
        else:
            return request_time, response.status_code, []

    except requests.exceptions.RequestException as e:
        end_time = time.time()
        request_time = end_time - start_time
        return request_time, 500, []


def basic_temp_entity_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Basic entity type query"""
    
    params = {"type": entity_type, "limit": 1000, "lastN": 50,"timerel": "before", "timeAt": "2026-09-10T12:25:57.457384Z","timeproperty": "modifiedAt"}

    request_time, status_code, entities = execute_query(
        base_url, "temporal/entities", params, headers
    )
    return request_time, status_code, len(entities)

def basic_entity_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Basic entity type query"""
    params = {"type": entity_type, "limit": 1000}

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)

def property_filter_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Query with property value filter"""
    # Filter for temperature > 50
    params = {"type": entity_type, "q": "temperature>50", "limit": 1000}

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def multiple_property_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Query with multiple property filters"""
    # Filter for temperature > 40 AND humidity < 60
    params = {"type": entity_type, "q": "temperature>40;humidity<60", "limit": 1000}

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def geo_query_nearby(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Geo query - entities near a point"""
    # Query entities within 5km of Madrid center
    params = {
        "type": entity_type,
        "georel": "near;maxDistance==5000",
        "geometry": "Point",
        "coordinates": "[-3.7038,40.4168]",
        "limit": 1000,
    }

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def geo_query_within(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Geo query - entities within a bounding box"""
    # Query entities within a bounding box around Madrid
    params = {
        "type": entity_type,
        "georel": "within",
        "geometry": "Polygon",
        "coordinates": "[[[-3.8,40.3],[-3.6,40.3],[-3.6,40.5],[-3.8,40.5],[-3.8,40.3]]]",
        "limit": 1000,
    }

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def attrs_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Query with specific attributes selection (attrs)"""
    params = {"type": entity_type, "attrs": "temperature,humidity", "limit": 1000}

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def pick_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Query with specific attributes selection (pick)"""
    params = {"type": entity_type, "pick": "temperature,humidity", "limit": 1000}

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def omit_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Query with attributes omission"""
    params = {"type": entity_type, "omit": "voltage,pressure", "limit": 1000}

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def id_pattern_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Query with ID pattern matching"""
    params = {
        "type": entity_type,
        "idPattern": ".*A.*",  # IDs containing 'A'
        "limit": 1000,
    }

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def combined_filter_query(
    base_url: str, entity_type: str, headers: Dict[str, str]
) -> Tuple[float, int, int]:
    """Query combining multiple filter types"""
    params = {
        "type": entity_type,
        "q": "temperature>30",
        "attrs": "temperature,location",
        "georel": "near;maxDistance==3000",
        "geometry": "Point",
        "coordinates": "[-3.7038,40.4168]",
        "limit": 1000,
    }

    request_time, status_code, entities = execute_query(
        base_url, "entities", params, headers
    )
    return request_time, status_code, len(entities)


def run_query_benchmark(
    query_func,
    query_name: str,
    base_url: str,
    entity_type: str,
    headers: Dict[str, str],
    iterations: int = 1000,
    csv_writer=None,
) -> Dict[str, Any]:
    """Run a query function multiple times and collect performance metrics"""

    print(f"\nRunning {query_name} ({iterations} iterations)...")

    times = []
    success_count = 0
    error_count = 0
    total_entities = 0
    detailed_results = []

    for i in range(iterations):
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{iterations}")

        try:
            request_time, status_code, entity_count = query_func(
                base_url, entity_type, headers
            )
            times.append(request_time)
            
            # Record detailed result for CSV
            result_row = {
                'timestamp': datetime.now().isoformat(),
                'query_name': query_name,
                'iteration': i + 1,
                'response_time': request_time,
                'response_time_ms': request_time * 1000,
                'status_code': status_code,
                'entity_count': entity_count,
                'success': status_code == 200
            }
            detailed_results.append(result_row)
            
            # Write to CSV if writer provided
            if csv_writer:
                csv_writer.writerow(result_row)

            if status_code == 200:
                success_count += 1
                total_entities += entity_count
            else:
                error_count += 1

        except Exception as e:
            error_count += 1
            print(f"  Exception in iteration {i + 1}: {e}")
            
            # Record error in CSV
            result_row = {
                'timestamp': datetime.now().isoformat(),
                'query_name': query_name,
                'iteration': i + 1,
                'response_time': 0,
                'response_time_ms': 0,
                'status_code': 500,
                'entity_count': 0,
                'success': False
            }
            detailed_results.append(result_row)
            
            if csv_writer:
                csv_writer.writerow(result_row)

        # Small delay to avoid overwhelming the server
        time.sleep(0.01)

    if times:
        metrics = {
            "query_name": query_name,
            "iterations": iterations,
            "success_count": success_count,
            "error_count": error_count,
            "total_entities": total_entities,
            "avg_entities_per_query": (
                total_entities / success_count if success_count > 0 else 0
            ),
            "avg_time": statistics.mean(times),
            "median_time": statistics.median(times),
            "min_time": min(times),
            "max_time": max(times),
            "std_dev": statistics.stdev(times) if len(times) > 1 else 0,
            "p95_time": sorted(times)[int(0.95 * len(times))] if times else 0,
            "p99_time": sorted(times)[int(0.99 * len(times))] if times else 0,
            "queries_per_second": success_count / sum(times) if sum(times) > 0 else 0,
            "detailed_results": detailed_results
        }
    else:
        metrics = {
            "query_name": query_name,
            "iterations": iterations,
            "success_count": 0,
            "error_count": error_count,
            "total_entities": 0,
            "avg_entities_per_query": 0,
            "avg_time": 0,
            "median_time": 0,
            "min_time": 0,
            "max_time": 0,
            "std_dev": 0,
            "p95_time": 0,
            "p99_time": 0,
            "queries_per_second": 0,
            "detailed_results": detailed_results
        }

    return metrics


def print_results(results: List[Dict[str, Any]]):
    """Print formatted benchmark results"""

    print("\n" + "=" * 120)
    print("NGSI-LD QUERY PERFORMANCE BENCHMARK RESULTS")
    print("=" * 120)

    header = f"{'Query Type':<25} {'Success':<8} {'Errors':<8} {'Avg Entities':<12} {'Avg Time (ms)':<15} {'Median (ms)':<13} {'P95 (ms)':<12} {'P99 (ms)':<12} {'QPS':<8}"
    print(header)
    print("-" * 120)

    for result in results:
        row = (
            f"{result['query_name']:<25} "
            f"{result['success_count']:<8} "
            f"{result['error_count']:<8} "
            f"{result['avg_entities_per_query']:<12.1f} "
            f"{result['avg_time']*1000:<15.1f} "
            f"{result['median_time']*1000:<13.1f} "
            f"{result['p95_time']*1000:<12.1f} "
            f"{result['p99_time']*1000:<12.1f} "
            f"{result['queries_per_second']:<8.1f}"
        )
        print(row)

    print("\n" + "=" * 120)
    print("Legend:")
    print("- Success: Number of successful queries (HTTP 200)")
    print("- Errors: Number of failed queries")
    print("- Avg Entities: Average number of entities returned per successful query")
    print("- Times are in milliseconds")
    print("- QPS: Queries Per Second (successful queries only)")


def generate_plotly_graphs(results: List[Dict[str, Any]], output_dir: str = "benchmark_results"):
    """Generate comprehensive Plotly graphs from benchmark results"""
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Prepare data for plotting
    all_detailed_data = []
    for result in results:
        all_detailed_data.extend(result['detailed_results'])
    
    df = pd.DataFrame(all_detailed_data)
    df_success = df[df['success'] == True].copy()
    
    # 1. Response Time Distribution by Query Type (Box Plot)
    fig1 = px.box(df_success, x='query_name', y='response_time_ms', 
                  title='Response Time Distribution by Query Type')
    fig1.update_xaxes(tickangle=45)
    fig1.update_layout(height=600)
    fig1.write_html(f"{output_dir}/response_time_distribution.html")
    
    # 2. Response Time Over Iterations (Line Plot)
    fig2 = px.line(df_success, x='iteration', y='response_time_ms', color='query_name',
                   title='Response Time Over Iterations')
    fig2.update_layout(height=600)
    fig2.write_html(f"{output_dir}/response_time_over_iterations.html")
    
    # 3. Average Response Time by Query Type (Bar Chart)
    avg_times = df_success.groupby('query_name')['response_time_ms'].mean().reset_index()
    fig3 = px.bar(avg_times, x='query_name', y='response_time_ms',
                  title='Average Response Time by Query Type')
    fig3.update_xaxes(tickangle=45)
    fig3.update_layout(height=600)
    fig3.write_html(f"{output_dir}/average_response_time.html")
    
    # 4. Success Rate by Query Type
    success_rates = df.groupby('query_name').agg({
        'success': ['count', 'sum']
    }).round(3)
    success_rates.columns = ['total', 'successful']
    success_rates['success_rate'] = (success_rates['successful'] / success_rates['total'] * 100)
    success_rates = success_rates.reset_index()
    
    fig4 = px.bar(success_rates, x='query_name', y='success_rate',
                  title='Success Rate by Query Type (%)')
    fig4.update_xaxes(tickangle=45)
    fig4.update_layout(height=600)
    fig4.write_html(f"{output_dir}/success_rate.html")
    
    # 5. Entity Count Distribution
    if df_success['entity_count'].sum() > 0:
        fig5 = px.box(df_success, x='query_name', y='entity_count',
                      title='Entity Count Distribution by Query Type')
        fig5.update_xaxes(tickangle=45)
        fig5.update_layout(height=600)
        fig5.write_html(f"{output_dir}/entity_count_distribution.html")
    
    # 6. Performance Summary Dashboard
    fig6 = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Avg Response Time (ms)', 'Success Rate (%)', 
                       'P95 Response Time (ms)', 'Queries Per Second'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    query_names = [r['query_name'] for r in results]
    
    # Average response time
    fig6.add_trace(
        go.Bar(x=query_names, y=[r['avg_time']*1000 for r in results], name="Avg Time"),
        row=1, col=1
    )
    
    # Success rate
    success_rates_data = [(r['success_count'] / r['iterations'] * 100) for r in results]
    fig6.add_trace(
        go.Bar(x=query_names, y=success_rates_data, name="Success Rate"),
        row=1, col=2
    )
    
    # P95 response time
    fig6.add_trace(
        go.Bar(x=query_names, y=[r['p95_time']*1000 for r in results], name="P95 Time"),
        row=2, col=1
    )
    
    # Queries per second
    fig6.add_trace(
        go.Bar(x=query_names, y=[r['queries_per_second'] for r in results], name="QPS"),
        row=2, col=2
    )
    
    fig6.update_layout(height=800, title_text="Performance Summary Dashboard", showlegend=False)
    fig6.update_xaxes(tickangle=45)
    fig6.write_html(f"{output_dir}/performance_dashboard.html")
    
    # 7. Heatmap of Response Times
    pivot_data = df_success.pivot_table(values='response_time_ms', 
                                       index='iteration', 
                                       columns='query_name', 
                                       aggfunc='mean')
    
    fig7 = px.imshow(pivot_data.T, 
                     title='Response Time Heatmap (Query Type vs Iteration)',
                     labels=dict(x="Iteration", y="Query Type", color="Response Time (ms)"),
                     aspect="auto")
    fig7.update_layout(height=600)
    fig7.write_html(f"{output_dir}/response_time_heatmap.html")
    
    print(f"\n📊 Plotly graphs generated in '{output_dir}' directory:")
    print(f"   - response_time_distribution.html")
    print(f"   - response_time_over_iterations.html")
    print(f"   - average_response_time.html")
    print(f"   - success_rate.html")
    if df_success['entity_count'].sum() > 0:
        print(f"   - entity_count_distribution.html")
    print(f"   - performance_dashboard.html")
    print(f"   - response_time_heatmap.html")


def main():
    # Configuration
    BASE_URL = "http://localhost:9090/ngsi-ld/v1"
    ENTITY_TYPE = "SensorDevice"
    ITERATIONS = 100
    
    # Create timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"benchmark_results_{timestamp}.csv"
    results_dir = f"benchmark_results_{timestamp}"

    headers = {
        "Accept": "application/ld+json",
        "Content-Type": "application/ld+json",
        # Add authentication headers here if needed:
        # 'Authorization': 'Bearer your_token_here'
    }

    print(f"NGSI-LD Query Performance Benchmark")
    print(f"Base URL: {BASE_URL}")
    print(f"Entity Type: {ENTITY_TYPE}")
    print(f"Iterations per query type: {ITERATIONS}")
    print(f"Results will be saved to: {csv_filename}")
    print("=" * 60)

    # Test connectivity
    try:
        response = requests.get(
            f"{BASE_URL.replace('/ngsi-ld/v1', '')}/ngsi-ld/v1/info/sourceIdentity", timeout=5
        )
        if response.status_code == 200:
            print("✓ Broker connectivity confirmed")
        else:
            print(f"⚠️  Broker returned status {response.status_code}")
    except requests.exceptions.RequestException:
        print("⚠️  Cannot reach broker. Proceeding anyway...")

    # Setup CSV file
    csv_fieldnames = ['timestamp', 'query_name', 'iteration', 'response_time', 
                     'response_time_ms', 'status_code', 'entity_count', 'success']
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=csv_fieldnames)
        csv_writer.writeheader()

        # Define query benchmarks
        benchmarks = [
            (basic_temp_entity_query, "Temp Query"),
            (property_filter_query, "Property Filter"),
            (basic_entity_query, "Basic Entity Query"),
            (multiple_property_query, "Multiple Properties"),
            (geo_query_nearby, "Geo Query (Near)"),
            (geo_query_within, "Geo Query (Within)"),
            (pick_query, "Attributes Pick"),
            (omit_query, "Attributes Omit"),
            (attrs_query, "Attributes Attrs"),
            (id_pattern_query, "ID Pattern"),
            (combined_filter_query, "Combined Filters"),
        ]

        results = []

        # Run benchmarks
        start_time = time.time()

        for query_func, query_name in benchmarks:
            result = run_query_benchmark(
                query_func, query_name, BASE_URL, ENTITY_TYPE, headers, ITERATIONS, csv_writer
            )
            results.append(result)

        total_time = time.time() - start_time

        # Print results
        print_results(results)

        print(f"\nTotal benchmark time: {total_time:.1f} seconds")
        print(f"Total queries executed: {len(benchmarks) * ITERATIONS}")
        print(f"📄 Detailed results saved to: {csv_filename}")

    # Generate Plotly graphs
    try:
        generate_plotly_graphs(results, results_dir)
    except Exception as e:
        print(f"⚠️  Error generating graphs: {e}")
        print("Make sure you have plotly and pandas installed: pip install plotly pandas")


if __name__ == "__main__":
    main()