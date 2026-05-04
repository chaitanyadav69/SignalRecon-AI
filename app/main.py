"""
Sentinel-Stream: Core System Orchestrator
=========================================
Manages distributed ingestion threads, telemetry metrics, and the 
real-time validation web interface.
"""

import os
import threading
import time
from flask import Flask, render_template, send_file, request, jsonify
from .consumer import TradeDataConsumer as DataStreamConsumer  # Aliased for masking
from .reconcile import ReconciliationEngine as ValidationEngine
from .report_generator import ReportGenerator as AnalyticsGenerator
from prometheus_client import start_http_server, Counter, Gauge, Histogram

app = Flask(__name__, template_folder='../reports/templates', static_folder='../reports')

# Distributed Infrastructure Config
KAFKA_NODES = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
DB_PATH = 'sqlite:///./reports/reconciliation.db'

# System Telemetry Metrics (Prometheus)
TOTAL_EVENTS_INGESTED = Counter('sentinel_total_events_processed', 'Total distributed records ingested')
VALIDATED_RECORDS = Counter('sentinel_validated_records_total', 'Total records with zero-drift')
DRIFT_DETECTION_COUNT = Counter('sentinel_drift_detected_total', 'Total records flagging integrity anomalies')
BUFFER_STATE_GAUGE = Gauge('sentinel_in_memory_buffer_size', 'Current size of the ingestion buffer')
LATENCY_HISTOGRAM = Histogram('sentinel_reconciliation_latency_seconds', 'Processing latency for cross-source validation')

# Component Initialization
validator = ValidationEngine(db_url=DB_PATH)
analytics = AnalyticsGenerator(db_url=DB_PATH, template_dir='./reports/templates')

# Injecting Telemetry Collectors into the Engine
validator.set_metrics_collectors(
    total_trades_counter=TOTAL_EVENTS_INGESTED,
    matched_trades_counter=VALIDATED_RECORDS,
    mismatched_trades_counter=DRIFT_DETECTION_COUNT,
    in_memory_store_size_gauge=BUFFER_STATE_GAUGE,
    reconciliation_latency_histogram=LATENCY_HISTOGRAM
)

active_threads = []

@app.route('/')
def dashboard_home():
    """Renders the real-time system integrity dashboard."""
    return analytics.generate_html_report()

@app.route('/api/integrity_status')
def get_system_status():
    """API endpoint for current validation state."""
    results_df = analytics.fetch_all_reconciliation_results()
    return jsonify(results_df.to_dict(orient='records'))

@app.route('/export/audit_trail')
def export_audit_csv():
    """Generates a verifiable CSV audit trail of all reconciliation events."""
    export_file = 'integrity_audit_trail.csv'
    analytics.generate_csv_report(filename=export_file)
    path = os.path.join(analytics.report_output_dir, export_file)
    return send_file(path, as_attachment=True, download_name='Sentinel_Integrity_Audit.csv', mimetype='text/csv')

def initialize_ingestion_layer():
    """Spawns asynchronous threads for heterogeneous data ingestion."""
    logger_msg = "Initializing distributed Kafka consumers..."
    print(logger_msg)
    
    # Primary Source Ingestion
    primary_stream = DataStreamConsumer(
        topic='executions',
        bootstrap_servers=KAFKA_NODES,
        group_id=f'sentinel_primary_{int(time.time())}',
        reconcile_engine=validator
    )
    # Secondary Source Validation Ingestion
    secondary_stream = DataStreamConsumer(
        topic='confirmations',
        bootstrap_servers=KAFKA_NODES,
        group_id=f'sentinel_validator_{int(time.time())}',
        reconcile_engine=validator
    )
    # Metadata Ingestion
    metadata_stream = DataStreamConsumer(
        topic='pnl_snapshot',
        bootstrap_servers=KAFKA_NODES,
        group_id=f'sentinel_metadata_{int(time.time())}',
        reconcile_engine=validator
    )

    active_threads.extend([primary_stream, secondary_stream, metadata_stream])

    for thread in active_threads:
        thread.start()
        time.sleep(0.5)

    print("Ingestion layer operational across all threads.")

def terminate_gracefully():
    """Ensures thread safety during system shutdown."""
    print("Terminating ingestion layers...")
    for thread in active_threads:
        thread.stop()
    for thread in active_threads:
        thread.join()
    print("All subsystems successfully decoupled.")

if __name__ == '__main__':
    # Start Telemetry Server for Prometheus scraping
    start_http_server(8000, addr='0.0.0.0')
    print("Telemetry endpoint established on port 8000.")

    # Execute Ingestion in Background
    orchestrator_thread = threading.Thread(target=initialize_ingestion_layer)
    orchestrator_thread.start()

    try:
        # Launch Dashboard Interface
        app.run(debug=False, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("System interrupt detected.")
    finally:
        terminate_gracefully()
        orchestrator_thread.join()
        print("Sentinel-Stream shut down successfully.")
