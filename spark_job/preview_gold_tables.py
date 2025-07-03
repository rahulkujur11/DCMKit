def preview_gold_tables(topic, base_path=GOLD_PATH, limit=10):
    """
    Read and display all Gold layer tables for a given topic.

    Args:
        topic (str): Topic name (e.g., 'demand-topic')
        base_path (str): Base Gold path
        limit (int): Number of rows to display per table

    Returns:
        dict[str, DataFrame]: Dictionary of table_name -> Spark DataFrame
    """
    topic_folder = TOPIC_TO_FOLDER.get(topic)
    if not topic_folder:
        raise ValueError(f"Unknown topic: {topic}")

    gold_topic_path = os.path.join(base_path, topic_folder)
    if not os.path.exists(gold_topic_path):
        raise FileNotFoundError(f"Gold folder not found: {gold_topic_path}")

    logger.info(f"🔍 Previewing Gold tables under: {gold_topic_path}")

    tables = {}
    for table_name in os.listdir(gold_topic_path):
        table_path = os.path.join(gold_topic_path, table_name)
        if not os.path.isdir(table_path):
            continue  # Skip non-folder files
        try:
            df = spark.read.format("delta").load(table_path)
            logger.info(f"📦 {table_name}: {df.count()} rows")
            df.show(limit, truncate=False)
            tables[table_name] = df
        except Exception as e:
            logger.error(f"⚠️ Could not load table {table_name}: {e}")
            logger.debug(traceback.format_exc())

    return tables
