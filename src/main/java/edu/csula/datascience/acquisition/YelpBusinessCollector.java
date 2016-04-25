package edu.csula.datascience.acquisition;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONObject;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An example of Collector implementation using Twitter4j with MongoDB Java driver
 */
public class YelpBusinessCollector implements Collector<JSONObject, JSONObject> {
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> businesses;

    public YelpBusinessCollector() {
        // establish database connection to MongoDB
        mongoClient = new MongoClient(); // to localhost

        // select `bd-example` as testing database
        database = mongoClient.getDatabase("cs594-spring2016");

        // select collection by name `tweets`
        businesses = database.getCollection("businesses");
    }

    @Override
    public Collection<JSONObject> mungee(Collection<JSONObject> src) {

        // Remove unneeded props
        for (JSONObject jobj : src) {
            if(jobj.containsKey("rating_img_url")){
                jobj.remove("rating_img_url");
            }

            if(jobj.containsKey("image_url")){
                jobj.remove("image_url");
            }

            if(jobj.containsKey("rating_img_url_large")){
                jobj.remove("rating_img_url_large");
            }

            if(jobj.containsKey("rating_img_url_small")){
                jobj.remove("rating_img_url_small");
            }
        }

        return src;
    }

    @Override
    public void save(Collection<JSONObject> data) {

        // Parse each JSONObject as string and convert to Document
        List<Document> documents = data.stream()
            .map(item -> Document.parse(item.toString()))
            .collect(Collectors.toList());

        System.out.println("Saving Documents to MongoDB: " + data.size());

        // Only add if we have documents to save
        if (documents.size() > 0) businesses.insertMany(documents);
    }
}
