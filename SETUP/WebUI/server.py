from flask import Flask
from flask import render_template
from flask_pymongo import PyMongo
from pymongo import MongoClient
from flask import Markup
from bson.json_util import dumps
from flask import jsonify
from flask import Response
from flask import request

app = Flask(__name__)

#username = urllib.parse.quote_plus('event')
#password = urllib.parse.quote_plus('123456')
#client = MongoClient('mongodb://%s:%s@192.168.8.128:27017/fdb' % (username, password))
#db = client.fdb
#SwData = db.SwitchData
app.config['MONGO_DBNAME'] = 'fdb'
app.config['MONGO_URI'] = 'mongodb://event:123456@192.168.8.128:27017/fdb'

mongo = PyMongo(app)

@app.route("/")
def main():
    return "Welcome to the first Flask App!"

@app.route("/search", methods=['GET'])
def databuf():
	qtype = request.args.get('q')
	data = mongo.db.SwitchData.find()
	print(qtype)
	if (qtype == 'HostData'):
		print (qtype)
		data = mongo.db.HostData.find({},{"_id":0})
	if(qtype == 'SwitchData'):
		print (qtype)
		data = mongo.db.SwitchData.find({},{"_id":0}).limit(10000)			
	if(qtype == 'Diagnostic'):
		data = mongo.db.DiagnosticData.find({},{"_id":0}).limit(100000)
	db_object = dumps(data)	

	resp = Response(response=db_object,
                    status=200,
                    mimetype="application/json")
	return resp

@app.route("/datatable")
def datatable():
	return render_template("bttables.html")
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
