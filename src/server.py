import logging

from flask import Flask
from flask_restful import Api

from api.resources.disk import Disk
from api.resources.heartbeat import Heartbeat
from api.resources.job import Job
from api.resources.monitor import Monitor
from api.resources.mount import Mount

logging .basicConfig(level=logging.DEBUG, format='%(asctime)s [%(name)s][%(levelname)s]: %(message)s', filename='server.log',
                     filemode='w')
log = logging.getLogger('werkzeug').setLevel(logging.WARNING)  # Suppress HTTP request logging

app = Flask(__name__)
api = Api(app)

restorations = {}
mounts = {}


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

api.add_resource(Heartbeat, '/api/heartbeat')
api.add_resource(Monitor, '/api/metric')
api.add_resource(Disk, '/api/disk', '/api/disk/<disk_id>')
api.add_resource(Job, '/api/job', '/api/job/<job_id>')
api.add_resource(Mount, '/api/mount', '/api/mount/<backup_id>')

if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True)
