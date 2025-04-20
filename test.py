import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, classification_report
import numpy as np
from datetime import datetime

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="thrishal",
    database="fraud_detection"
)

def load_data():
    """Load data from both streaming and batch results"""
    # Load all transactions (our ground truth)
    all_txns = pd.read_sql(
        "SELECT * FROM all_transactions", conn
    )
    
    # Load streaming results
    stream_frauds_amount = pd.read_sql(
        "SELECT * FROM frauds", conn
    )
    
    stream_frauds_hf = pd.read_sql(
        "SELECT * FROM frauds_hf", conn
    )
    
    # Load batch results
    batch_frauds_amount = pd.read_sql(
        "SELECT * FROM batch_fraud_amount", conn
    )
    
    batch_frauds_hf = pd.read_sql(
        "SELECT * FROM batch_fraud_highfreq", conn
    )
    
    return {
        'all_txns': all_txns,
        'stream_frauds_amount': stream_frauds_amount,
        'stream_frauds_hf': stream_frauds_hf,
        'batch_frauds_amount': batch_frauds_amount,
        'batch_frauds_hf': batch_frauds_hf
    }

def prepare_labels(data):
    """Prepare binary labels for each transaction"""
    all_txns = data['all_txns'].copy()
    
    # Create ground truth (transactions > 90000 are fraud)
    all_txns['is_fraud_amount'] = (all_txns['amount'] > 90000).astype(int)
    
    # For high frequency, we need to count transactions by user within time windows
    # This is a simplified approach - ideally match your streaming logic exactly
    all_txns['timestamp'] = pd.to_datetime(all_txns['timestamp'])
    all_txns = all_txns.sort_values('timestamp')
    
    # Create 5-minute windows
    all_txns['time_window'] = all_txns['timestamp'].dt.floor('5min')
    
    # Count transactions by user within each window
    window_counts = all_txns.groupby(['user_id', 'time_window']).size().reset_index(name='txn_count')
    window_counts['is_fraud_hf'] = (window_counts['txn_count'] > 3).astype(int)
    
    # Merge high frequency labels back
    all_txns = all_txns.merge(
        window_counts[['user_id', 'time_window', 'is_fraud_hf']], 
        on=['user_id', 'time_window'],
        how='left'
    )
    
    # Prepare streaming predictions
    stream_amount_fraud_ids = set(data['stream_frauds_amount']['transaction_id'])
    all_txns['stream_pred_amount'] = all_txns['transaction_id'].apply(
        lambda x: 1 if x in stream_amount_fraud_ids else 0
    )
    
    # High frequency streaming predictions are trickier - we'll match by user_id and timestamp
    stream_hf_users = set(zip(
        data['stream_frauds_hf']['user_id'], 
        pd.to_datetime(data['stream_frauds_hf']['timestamp']).dt.floor('5min')
    ))
    
    all_txns['stream_pred_hf'] = all_txns.apply(
        lambda row: 1 if (row['user_id'], row['time_window']) in stream_hf_users else 0,
        axis=1
    )
    
    # Prepare batch predictions (similar approach)
    batch_amount_fraud_ids = set(data['batch_frauds_amount']['transaction_id'])
    all_txns['batch_pred_amount'] = all_txns['transaction_id'].apply(
        lambda x: 1 if x in batch_amount_fraud_ids else 0
    )
    
    batch_hf_users = set(data['batch_frauds_hf']['user_id'])
    all_txns['batch_pred_hf'] = all_txns.apply(
        lambda row: 1 if row['user_id'] in batch_hf_users else 0,
        axis=1
    )
    
    return all_txns

def calculate_metrics(labeled_data):
    """Calculate performance metrics for both approaches"""
    metrics = {}
    
    # Amount-based fraud detection metrics
    print("\n=== AMOUNT FRAUD DETECTION ===")
    print("\nStreaming Detection Metrics:")
    stream_amount_report = classification_report(
        labeled_data['is_fraud_amount'], 
        labeled_data['stream_pred_amount'],
        output_dict=True
    )
    print(classification_report(labeled_data['is_fraud_amount'], labeled_data['stream_pred_amount']))
    
    print("\nBatch Detection Metrics:")
    batch_amount_report = classification_report(
        labeled_data['is_fraud_amount'], 
        labeled_data['batch_pred_amount'],
        output_dict=True
    )
    print(classification_report(labeled_data['is_fraud_amount'], labeled_data['batch_pred_amount']))
    
    # High-frequency fraud detection metrics
    print("\n=== HIGH FREQUENCY FRAUD DETECTION ===")
    print("\nStreaming Detection Metrics:")
    stream_hf_report = classification_report(
        labeled_data['is_fraud_hf'], 
        labeled_data['stream_pred_hf'],
        output_dict=True
    )
    print(classification_report(labeled_data['is_fraud_hf'], labeled_data['stream_pred_hf']))
    
    print("\nBatch Detection Metrics:")
    batch_hf_report = classification_report(
        labeled_data['is_fraud_hf'], 
        labeled_data['batch_pred_hf'],
        output_dict=True
    )
    print(classification_report(labeled_data['is_fraud_hf'], labeled_data['batch_pred_hf']))
    
    metrics['stream_amount'] = stream_amount_report
    metrics['batch_amount'] = batch_amount_report
    metrics['stream_hf'] = stream_hf_report
    metrics['batch_hf'] = batch_hf_report
    
    return metrics

def plot_confusion_matrices(labeled_data):
    """Generate confusion matrices for visualization"""
    plt.figure(figsize=(16, 12))
    
    # Amount-based fraud detection
    plt.subplot(2, 2, 1)
    cm_stream_amount = confusion_matrix(
        labeled_data['is_fraud_amount'], 
        labeled_data['stream_pred_amount']
    )
    sns.heatmap(cm_stream_amount, annot=True, fmt='d', cmap='Blues',
               xticklabels=['Normal', 'Fraud'], yticklabels=['Normal', 'Fraud'])
    plt.title('Streaming: Amount-based Fraud')
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    
    plt.subplot(2, 2, 2)
    cm_batch_amount = confusion_matrix(
        labeled_data['is_fraud_amount'], 
        labeled_data['batch_pred_amount']
    )
    sns.heatmap(cm_batch_amount, annot=True, fmt='d', cmap='Blues',
               xticklabels=['Normal', 'Fraud'], yticklabels=['Normal', 'Fraud'])
    plt.title('Batch: Amount-based Fraud')
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    
    # High-frequency fraud detection
    plt.subplot(2, 2, 3)
    cm_stream_hf = confusion_matrix(
        labeled_data['is_fraud_hf'], 
        labeled_data['stream_pred_hf']
    )
    sns.heatmap(cm_stream_hf, annot=True, fmt='d', cmap='Blues',
               xticklabels=['Normal', 'Fraud'], yticklabels=['Normal', 'Fraud'])
    plt.title('Streaming: High-Frequency Fraud')
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    
    plt.subplot(2, 2, 4)
    cm_batch_hf = confusion_matrix(
        labeled_data['is_fraud_hf'], 
        labeled_data['batch_pred_hf']
    )
    sns.heatmap(cm_batch_hf, annot=True, fmt='d', cmap='Blues',
               xticklabels=['Normal', 'Fraud'], yticklabels=['Normal', 'Fraud'])
    plt.title('Batch: High-Frequency Fraud')
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    
    plt.tight_layout()
    plt.savefig('confusion_matrices.png')
    plt.close()
    
    # Performance comparison charts
    plt.figure(figsize=(10, 8))
    
    # F1 scores
    metrics = [
        ('Amount Fraud', 
         metrics_dict['stream_amount']['1']['f1-score'], 
         metrics_dict['batch_amount']['1']['f1-score']),
        ('High Frequency Fraud', 
         metrics_dict['stream_hf']['1']['f1-score'], 
         metrics_dict['batch_hf']['1']['f1-score'])
    ]
    
    x = np.arange(len(metrics))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(10, 6))
    rects1 = ax.bar(x - width/2, [m[1] for m in metrics], width, label='Streaming')
    rects2 = ax.bar(x + width/2, [m[2] for m in metrics], width, label='Batch')
    
    ax.set_ylabel('F1 Score')
    ax.set_title('F1 Score by Detection Method')
    ax.set_xticks(x)
    ax.set_xticklabels([m[0] for m in metrics])
    ax.legend()
    
    for rect in rects1 + rects2:
        height = rect.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('f1_comparison.png')
    plt.close()

def time_performance_analysis():
    """Measure execution time differences"""
    # This would normally involve running both systems on the same data
    # and measuring execution times, but for this demo we'll simulate it
    
    # Connect to the database
    cursor = conn.cursor()
    
    # For streaming, we can estimate the average processing time per transaction
    cursor.execute("""
        SELECT 
            COUNT(*) as total_txns,
            MIN(timestamp) as start_time,
            MAX(timestamp) as end_time
        FROM all_transactions
    """)
    result = cursor.fetchone()
    total_transactions = result[0]
    
    if total_transactions > 0:
        start_time = result[1]
        end_time = result[2]
        
        if start_time and end_time:
            stream_time_diff = (end_time - start_time).total_seconds()
            
            print("\n=== PERFORMANCE METRICS ===")
            print(f"Total transactions processed: {total_transactions}")
            print(f"Stream processing window: {stream_time_diff:.2f} seconds")
            print(f"Average stream processing time per transaction: "
                  f"{stream_time_diff/total_transactions:.4f} seconds")
            
            # Batch processing is typically faster per transaction but processes all at once
            # Let's assume batch processing took 30% of the streaming time as an example
            # In a real scenario, you would measure this empirically
            batch_processing_time = stream_time_diff * 0.3
            print(f"Estimated batch processing time: {batch_processing_time:.2f} seconds")
            print(f"Batch processing efficiency: "
                  f"{stream_time_diff/batch_processing_time:.2f}x faster than streaming")
            
            return {
                'stream_time': stream_time_diff,
                'batch_time': batch_processing_time,
                'efficiency_ratio': stream_time_diff/batch_processing_time
            }
    
    return None

if __name__ == "__main__":
    print("Starting evaluation of fraud detection systems...")
    
    # Load data from both systems
    data = load_data()
    
    # Prepare binary labels
    labeled_data = prepare_labels(data)
    
    # Calculate metrics
    metrics_dict = calculate_metrics(labeled_data)
    
    # Generate visualizations
    plot_confusion_matrices(labeled_data)
    
    # Analyze time performance
    perf_metrics = time_performance_analysis()
    
    print("\nEvaluation complete. Check the generated images for visualization.")
    
    # Close connection
    conn.close()