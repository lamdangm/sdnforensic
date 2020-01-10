package net.floodlightcontroller.statistics;

import java.util.Arrays;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDb {
	@SuppressWarnings("deprecation")
	private static final Logger logger = LoggerFactory.getLogger(MongoDb.class.getName());

	private String User;
	private String Database;
	private String Pwd;
	protected MongoClient mgc;
	
	protected Block<Document> printBlock = new Block<Document>() {
	       @Override
	       public void apply(final Document document) {
	           logger.info("{}",document.toJson().toString());
	       }
	};
	
	MongoDb(String Ip,int Port,String Username, String DatabaseName, String PassWord){
		User = Username;
		Database = DatabaseName;
		Pwd = PassWord;
		mgc = Connect(Ip,Port);
	}
	
	private MongoClient Connect(String HostIp,int Port) {
		MongoClient mongoClient = null;
		try {
			MongoCredential mongoCred = MongoCredential.createCredential(User, Database, Pwd.toCharArray());
    		mongoClient = new MongoClient(new ServerAddress(HostIp, Port), Arrays.asList(mongoCred));
		}catch (MongoException e) {
			logger.error("Connect DB Fail",e);
		}
		return mongoClient;
	}
	
	public void QueryCollection(String DatabaseName, String CollectionName){  
		MongoDatabase mongodb = mgc.getDatabase(DatabaseName);
		MongoCollection<Document> collection = mongodb.getCollection(CollectionName);
		collection.find().forEach(printBlock);
	}
	public MongoCollection<Document> GetCollection(String DatabaseName, String CollectionName){  
		MongoDatabase mongodb = mgc.getDatabase(DatabaseName);
		MongoCollection<Document> collection = mongodb.getCollection(CollectionName);
		return collection;
	}
	public void Disconect() {
		mgc.close();
	}
}
