package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import fi.foyt.foursquare.api.entities.Category;
import fi.foyt.foursquare.api.entities.CompactVenue;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import twitter4j.Status;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An example of Collector implementation using Twitter4j with MongoDB Java driver
 */
public class FoursquareCollector implements Collector<JSONObject, CompactVenue> {

    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> venues;

    public FoursquareCollector() {
        // establish database connection to MongoDB
        mongoClient = new MongoClient();

        // select `bd-example` as testing database
        database = mongoClient.getDatabase("cs594-spring2016");

        // select collection by name `tweets`
        venues = database.getCollection("businesses");
    }

    @Override
    public Collection<JSONObject> mungee(Collection<CompactVenue> src) {

        List<JSONObject> list = Lists.newArrayList();

        for (CompactVenue venue : src) {
            JSONObject json_venue = new JSONObject();
            json_venue.put("name", venue.getName());
            json_venue.put("phone", venue.getContact().getPhone());
            json_venue.put("is_claimed", venue.getVerified());

            JSONArray json_venue_categories = new JSONArray();
            for(Category cat : venue.getCategories()){
                json_venue_categories.add(cat.getName());
            }
            json_venue.put("categories", json_venue_categories);

            JSONObject json_venue_location = new JSONObject();
            json_venue_location.put("country_code", venue.getLocation().getCountry());
            JSONArray json_venue_location_addr_array = new JSONArray();
            json_venue_location_addr_array.add(venue.getLocation().getAddress());
            json_venue_location.put("address", json_venue_location_addr_array);
            json_venue_location.put("city", venue.getLocation().getCity());
            json_venue_location.put("postal_code", venue.getLocation().getPostalCode());
            json_venue_location.put("state_code", venue.getLocation().getState());
            JSONObject json_venue_location_coordinate = new JSONObject();
            json_venue_location_coordinate.put("latitude", venue.getLocation().getLat());
            json_venue_location_coordinate.put("longitude", venue.getLocation().getLng());
            json_venue.put("location", json_venue_location);

            list.add(json_venue);
        }

        return list;
    }

    @Override
    public void save(Collection<JSONObject> data) {
        // Parse each JSONObject as string and convert to Document
        List<Document> documents = data.stream()
                .map(item -> Document.parse(item.toString()))
                .collect(Collectors.toList());

        System.out.println("Saving Documents to MongoDB: " + data.size());

        // Only add if we have documents to save
        if (documents.size() > 0) venues.insertMany(documents);
    }
}
