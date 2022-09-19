package bank.util;

import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.springframework.data.mongodb.core.MongoTemplate;


public class PushData {
    public static void pushDataToMongodb(String data) throws JsonParseException {
        MongoTemplate mongo = new MongoTemplate(MongoClients.create("mongodb://192.168.1.12"), "bank");
        Document bsonData = Document.parse(data);
        mongo.insert(bsonData, "account");
    }
}
