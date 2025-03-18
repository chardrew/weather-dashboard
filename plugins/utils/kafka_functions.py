def log_dag_trigger(message):
    """Kafka message processing function"""
    print(f"🔔 Received Kafka message: {message}")
    return True