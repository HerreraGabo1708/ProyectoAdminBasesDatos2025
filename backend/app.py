from flask import Flask, jsonify
from flask_cors import CORS  # opcional si usas proxy
from backend.monitoring import (
    get_cpu_memory, get_storage, get_top_queries,
    get_last_backup, recalculate_statistics, get_invalid_objects
)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})  # útil si algún día llamas sin proxy

@app.get('/api/cpu_memory')
def cpu_memory():
    return jsonify(get_cpu_memory())

@app.get('/api/storage')
def storage():
    return jsonify(get_storage())

@app.get('/api/top_queries')
def top_queries():
    return jsonify(get_top_queries())

@app.get('/api/last_backup')
def last_backup():
    return jsonify(get_last_backup())

@app.post('/api/recalculate_stats')
def recalculate_stats():
    recalculate_statistics()
    return jsonify({"message": "Estadísticas recalculadas exitosamente"})

@app.get('/api/invalid_objects')
def invalid_objects():
    return jsonify(get_invalid_objects())

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)  # puerto fijo para el proxy
