import pyodbc
from db import get_db_connection

def get_cpu_memory():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT cpu_percent, memory_usage
        FROM sys.dm_os_ring_buffers
        WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER'
        AND record_id = (SELECT MAX(record_id) FROM sys.dm_os_ring_buffers);
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"cpu_usage": result[0], "memory_usage": result[1]}

def get_storage():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT df.name AS FileName, df.size / 128.0 AS SizeMB
        FROM sys.master_files df;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    storage_data = [{"file_name": row[0], "size_mb": row[1]} for row in result]
    return storage_data

def get_top_queries():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT TOP 5
            total_worker_time / 1000.0 AS total_cpu_time_ms,
            execution_count,
            total_worker_time,
            total_elapsed_time / 1000.0 AS total_elapsed_time_seconds,
            (total_worker_time / execution_count) / 1000.0 AS avg_cpu_time_ms,
            SUBSTRING(qt.text, statement_start_offset / 2, 
                (CASE 
                    WHEN statement_end_offset = -1 
                    THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2 
                    ELSE statement_end_offset 
                END - statement_start_offset) / 2) AS query_text
        FROM sys.dm_exec_query_stats AS qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
        ORDER BY total_worker_time DESC;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    queries_data = [{"cpu_time_ms": row[0], "query": row[5]} for row in result]
    return queries_data

def get_last_backup():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT
            database_name,
            MAX(backup_finish_date) AS LastBackupDate
        FROM msdb.dbo.backupset
        WHERE type = 'D'
        GROUP BY database_name;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    backup_data = [{"database_name": row[0], "last_backup": row[1]} for row in result]
    return backup_data

def recalculate_statistics():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "EXEC sp_updatestats;"
    cursor.execute(query)
    conn.commit()

def get_invalid_objects():
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT 
            OBJECT_NAME(object_id) AS object_name,
            type_desc
        FROM sys.objects
        WHERE is_ms_shipped = 0
          AND OBJECTPROPERTY(object_id, 'IsValid') = 0;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    invalid_objects = [{"object_name": row[0], "type": row[1]} for row in result]
    return invalid_objects
