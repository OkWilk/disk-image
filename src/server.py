from flask import Flask
from flask_restful import Api, Resource, abort, reqparse, fields, marshal_with
from api.resources.disk import Disk
from api.resources.job import Job, JobDetails
from api.resources.heartbeat import Heartbeat

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
api.add_resource(Disk, '/api/disk', '/api/disk/<disk_id>')
api.add_resource(Job, '/api/job')
api.add_resource(JobDetails, '/api/job/<job_id>')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', threaded=True)
