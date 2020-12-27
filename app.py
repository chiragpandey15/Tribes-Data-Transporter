from flask import Flask
from flask_apscheduler import APScheduler

from gremlin_python.driver import client
from google.cloud import storage
from datetime import datetime
import os
import json
import datetime

app= Flask(__name__)
scheduler = APScheduler() #Initialise scheduler


def listToString(label):
	
	#In order to add multiple label, add it as property with key,value as {<label>:Label}
	res=''
	for elem in label:
		res=res+".property('"+str(elem)+"','Label')"

	return res


def propertyToString(property):

	res = ''.join("property('"+str(key) +"','" + str(val)+"')." for key, val in property.items()) 
	return res[:len(res)-1] #return the string by removing '.' from the end



#Submit Query and return result if successfully else -1
def submitQuery(client,query): 

	try:
		callback = client.submitAsync(query)

		if callback.result() is None:
			return []
		else:
			return callback.result().one()

	except Exception as e:
		print (e)

	return -1	


def addRelation(data,client):
	

	#Check if relationship exist
	query="g.V().has('"+data['FromLabel']+"','Label').has('IdObject','"+data['FromIdObject']\
	+"').outE('"+data['Type']+"').inV().has('"+data['ToLabel']+"','Label').has('IdObject','"+data['ToIdObject']+"')"

	res=submitQuery(client,query)

	if res==-1:
		return res # Some error occurred

	
	if len(res)==0: # Relationship does not exist

		#Check if left node exist
		query="g.V().has('"+data['FromLabel']+"','Label').has('IdObject','"+data['FromIdObject']+"')"
		res=submitQuery(client,query)

		if res==-1:
			return res 
		
		if len(res)==0: #Node does not exist

			timeCreated=datetime.datetime.now()

			query="g.addV().property('"+data['FromLabel']+"','Label').property('IdObject','"+data['FromIdObject']\
			+"').property('TimeCreated','"+timeCreated+"')"
			
			if submitQuery(client,query)==-1: #Some error occured
				return -1

		
	 	#Check if right node exist
		query="g.V().has('"+data['ToLabel']+"','Label').has('IdObject','"+data['ToIdObject']+"')"
		res=submitQuery(client,query)
		
		if res==-1:
			return res

		if len(res)==0:
			query="g.addV().property('"+data['ToLabel']+"','Label').property('IdObject','"+data['ToIdObject']\
			+"').property('TimeCreated','"+timeCreated+"')"
			
			if submitQuery(client,query)==-1:
				return -1

			
		
	elif data['DeDuplication']=='TRUE': #No need to add relation as relationship already exist
		return 0

	# Add relation
	query="g.V().has('"+data['FromLabel']+"','Label').has('IdObject','"+data['FromIdObject']\
	+"').addE('"+data['Type']+"')."+propertyToString(data['Property'])+".property('id','"+data['IdUnique']+"')"+\
	".to(g.V().has('"+data['ToLabel']+"','Label').has('IdObject','"+data['ToIdObject']+"'))"

	return submitQuery(client,query)
	



def addNode(data,client):

	#Add Node
	query = 'g.addV()'+listToString(data['Label'])+'.'+propertyToString(data['Property'])+".property('id','"+data['IdUnique']+"')"
	return submitQuery(client,query)



def sync():
	
	#Latest TimeStamp of the file that was processed
	LAST_PROCESS_TIME=os.environ['LAST_PROCESS_TIME']
	date_time_obj = datetime. strptime(LAST_PROCESS_TIME, '%Y-%m-%d %H:%M:%S.%f%z')

	latest=date_time_obj #Stores the lastest time during this sync process.


	#DbClient to connect to Cosmos Database using gremlin API
	DbClient = client.Client(
        'wss://'+os.environ['ENDPOINT']+'.gremlin.cosmos.azure.com' + ':443/', 'g',
        username="/dbs/" + os.environ['DATABASE'] + "/colls/" + os.environ['COLLECTION'],
        password=os.environ['PASSWORD']
    )
	
	# Instantiate a Google Cloud Storage client and specify required bucket
	storage_client = storage.Client.create_anonymous_client()
	bucket = storage_client.bucket(os.environ['BUCKET_NAME'])


	#Get the details of all the files in the bucket and then download the file to read it using json.load() method
	filename = list(bucket.list_blobs(prefix=os.environ['FOLDER']))
	for file in filename:
		
		#Check if the file is a json file and if it is a new file
		if file.name.endswith('.json') and date_time_obj<file.updated:

			#download the file and store in data variable
			blob = bucket.blob(file.name)
			data = json.loads(blob.download_as_string())

			#For all object find the kind of json object and call the required function
			for obj in data:
				if obj['Kind']=='node':
					if addNode(obj,DbClient)==-1: #Some error occured
						print ("Error occured in file "+file.name+" on line that contains json object with ID="+obj['IdUnique'])
						return
				else:
					if addRelation(obj,DbClient)==-1:
						print ("Error occured in file "+file.name+" on line that contains json object with ID="+obj['IdUnique'])
						return
		
			if latest<file.updated: #Ensure that latest variable stores the lastest time stamp of the file being read
				latest=file.updated


	os.environ['LAST_PROCESS_TIME']=str(latest) #Store the latest variable in order to use it in other sync




if __name__=="__main__":

	scheduler.add_job(id='sync',func=sync,trigger='interval',hours=24) #Schedule to run sync function every 24 hours
	scheduler.start()

	#Store data required to read files from Google storage 
	os.environ['BUCKET_NAME']='data-engineering-intern-data'
	os.environ['FOLDER']='graph-data/'


	#Store data required to use Gremlin API
	os.environ['ENDPOINT']='intership-assignments-2020'	
	os.environ['DATABASE']='graph-data-chirag-pandey'
	os.environ['COLLECTION']='graph-data-chirag-pandey'
	os.environ['PASSWORD']='9bpwqQIoaLegfAtUtTJIN8F4bsSPA1bcpqxBVBl0S7eOvRXAp5IdMIPWITj3qGhGVJsLzwk1s4hu3annNsWSag=='


	#Store the timestamp for latest process time of the file (Required to keep track of the file). It is set as a smallest value possible
	os.environ['LAST_PROCESS_TIME']='1900-01-01 00:00:00.000000+00:00'


	sync()


	app.run(debug=True) 
