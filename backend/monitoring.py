import traceback
from db import get_db_connection

def _safe_fetchone(cur):
    try:
        row = cur.fetchone()
        return row
    except Exception:
        return None

def get_cpu_memory():
    """Dejamos la versión que ya te funcionó (CPU ~100) + memoria realista."""
    conn = get_db_connection()
    cur = conn.cursor()

    # CPU (ring buffer, tu versión funcional)
    cpu_sql = r"""
    ;WITH parsed AS (
      SELECT
        CONVERT(xml, record).value('(./Record/@id)[1]','int') AS record_id,
        CONVERT(xml, record).value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]','int') AS system_idle
      FROM sys.dm_os_ring_buffers
      WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
        AND CAST(record AS nvarchar(max)) LIKE N'%<SystemHealth>%'
    )
    SELECT TOP 1 ISNULL(100 - system_idle, 100) AS overall_cpu
    FROM parsed
    ORDER BY record_id DESC;
    """
    try:
        cur.execute(cpu_sql)
        row = _safe_fetchone(cur)
        cpu_usage = float(row[0]) if row and row[0] is not None else 100.0
    except Exception:
        cpu_usage = 100.0

    # Memoria (más estable)
    mem_usage = 0.0
    try:
        cur.execute("""
          SELECT CAST(100.0 * committed_kb / NULLIF(committed_target_kb,0) AS float)
          FROM sys.dm_os_sys_info;
        """)
        row = _safe_fetchone(cur)
        if row and row[0] is not None:
            mem_usage = float(row[0])
        else:
            raise Exception("sys_info null")
    except Exception:
        try:
            cur.execute("SELECT memory_utilization_percentage FROM sys.dm_os_process_memory;")
            row = _safe_fetchone(cur)
            mem_usage = float(row[0]) if row and row[0] is not None else 0.0
        except Exception:
            mem_usage = 0.0

    # Clamp 0..100
    cpu_usage = max(0.0, min(100.0, cpu_usage))
    mem_usage = max(0.0, min(100.0, mem_usage))
    return {"cpu_usage": cpu_usage, "memory_usage": mem_usage}

def get_storage():
    """
    Tamaño de archivos por base, en MB.
    size = páginas de 8KB, por eso /128 -> MB
    """
    conn = get_db_connection()
    cur = conn.cursor()
    sql = """
    SELECT
        DB_NAME(mf.database_id)     AS database_name,
        mf.name                     AS file_name,
        mf.type_desc                AS file_type,
        mf.physical_name            AS physical_name,
        CAST(mf.size/128.0 AS decimal(18,2)) AS size_mb
    FROM sys.master_files AS mf
    ORDER BY mf.database_id, mf.file_id;
    """
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [
            {
                "database_name": r[0],
                "file_name": r[1],
                "file_type": r[2],
                "physical_name": r[3],
                "size_mb": float(r[4]),
            } for r in rows
        ]
    except Exception as e:
        return {"error": "storage_failed", "detail": str(e), "trace": traceback.format_exc()}

def get_top_queries(top_n: int = 5):
    """
    TOP consultas por CPU (total_worker_time). Devuelve texto y CPU acumulada.
    Requiere VIEW SERVER STATE.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    sql = f"""
    SELECT TOP ({top_n})
        (qs.total_worker_time/1000.0)                                     AS total_cpu_time_ms,
        qs.execution_count,
        (qs.total_worker_time/NULLIF(qs.execution_count,0))/1000.0        AS avg_cpu_time_ms,
        SUBSTRING(qt.text,
                  (qs.statement_start_offset/2) + 1,
                  (CASE WHEN qs.statement_end_offset = -1
                        THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2
                        ELSE qs.statement_end_offset END
                   - qs.statement_start_offset)/2 + 1)                    AS query_text
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
    ORDER BY qs.total_worker_time DESC;
    """
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [
            {
                "cpu_time_ms": float(r[0]) if r[0] is not None else 0.0,
                "execution_count": int(r[1]) if r[1] is not None else 0,
                "avg_cpu_time_ms": float(r[2]) if r[2] is not None else 0.0,
                "query": (r[3] or "").strip()
            } for r in rows
        ]
    except Exception as e:
        return {"error": "top_queries_failed", "detail": str(e), "trace": traceback.format_exc()}

def get_last_backup():
    """
    Último backup completo (D) por base. Requiere acceso a msdb.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    sql = """
    SELECT
        bs.database_name,
        MAX(bs.backup_finish_date) AS last_backup
    FROM msdb.dbo.backupset AS bs
    WHERE bs.type = 'D'
    GROUP BY bs.database_name
    ORDER BY bs.database_name;
    """
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [
            {
                "database_name": r[0],
                "last_backup": r[1].isoformat() if r[1] is not None else None
            } for r in rows
        ]
    except Exception as e:
        return {"error": "last_backup_failed", "detail": str(e), "trace": traceback.format_exc()}

def recalculate_statistics():
    """
    Actualiza estadísticas en todas las bases actuales (sp_updatestats por DB).
    Ejecutar como POST en la API.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Itera DBs de usuario (excluye tempdb y bases del sistema)
        cur.execute("""
          SELECT name
          FROM sys.databases
          WHERE database_id > 4  -- 1..4 = master, tempdb, model, msdb
            AND state = 0;       -- ONLINE
        """)
        dbs = [row[0] for row in cur.fetchall()]
        for db in dbs:
            cur.execute(f"EXEC [{db}].sys.sp_updatestats;")
        conn.commit()
        return {"message": "Estadísticas recalculadas exitosamente", "databases": dbs}
    except Exception as e:
        conn.rollback()
        return {"error": "recalculate_failed", "detail": str(e), "trace": traceback.format_exc()}

def get_invalid_objects():
    """
    'Objetos inválidos': objetos con dependencias no resueltas (vista/proc que referencia algo que no existe).
    Esto emula el concepto de 'invalid objects' de Oracle.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    sql = """
    SELECT
        OBJECT_SCHEMA_NAME(d.referencing_id) AS schema_name,
        OBJECT_NAME(d.referencing_id)        AS object_name,
        o.type_desc,
        d.referenced_entity_name             AS missing_reference
    FROM sys.sql_expression_dependencies AS d
    JOIN sys.objects AS o
      ON o.object_id = d.referencing_id
    WHERE d.referenced_id IS NULL                 -- referencia no resuelta
      AND d.is_ambiguous = 0
      AND o.is_ms_shipped = 0
    ORDER BY schema_name, object_name;
    """
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return [
            {
                "schema": r[0],
                "object_name": r[1],
                "type": r[2],
                "missing_reference": r[3]
            } for r in rows
        ]
    except Exception as e:
        return {"error": "invalid_objects_failed", "detail": str(e), "trace": traceback.format_exc()}
