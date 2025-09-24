from flask import Flask, jsonify
from src.monitoring import get_cpu_memory, get_storage, get_top_queries, get_last_backup, recalculate_statistics, get_invalid_objects

app = Flask(__name__)

@app.route('/cpu_memory')
def cpu_memory():
    cpu_memory_data = get_cpu_memory()
    return jsonify(cpu_memory_data)

@app.route('/storage')
def storage():
    storage_data = get_storage()
    return jsonify(storage_data)

@app.route('/top_queries')
def top_queries():
    queries_data = get_top_queries()
    return jsonify(queries_data)

@app.route('/last_backup')
def last_backup():
    backup_data = get_last_backup()
    return jsonify(backup_data)

@app.route('/recalculate_stats')
def recalculate_stats():
    recalculate_statistics()
    return jsonify({"message": "Estadísticas recalculadas exitosamente"})

@app.route('/invalid_objects')
def invalid_objects():
    invalid_objects_data = get_invalid_objects()
    return jsonify(invalid_objects_data)

if __name__ == '__main__':
    app.run(debug=True)
