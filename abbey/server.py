import flask
import flask_sqlalchemy
from flask import request

# Create the Flask application and the Flask-SQLAlchemy object.
app = flask.Flask(__name__)
app.config['DEBUG'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + "server.db"
db = flask_sqlalchemy.SQLAlchemy(app)

class Schema(db.Model):
    #Primary key must be integer or unicode.
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    version = db.Column(db.Integer)

class Dataset(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    subname = db.Column(db.Text)
    version = db.Column(db.Integer)
    host = db.Column(db.Integer)
    repo_path = db.Column(db.Integer)

# Create the database tables.
db.create_all()


@app.route('/dataset/create', methods=['POST'])
def create_dataset():
    payload = request.get_json()
    #name, subname, host, and repo_path should all be in here
    name = payload['name']
    subname = payload['subname']
    host = payload['host']
    repo_path = payload['repo_path']
    #find all existing datasets with that name and subname
    #get the next one
    #we create the version and give it a global ID

@app.route('/schema/create', methods=['POST'])
def create_schema():
    pass


# start the flask loop
app.run()


# name, subname, machine