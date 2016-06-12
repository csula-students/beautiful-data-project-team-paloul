package edu.csula.datascience.ingest;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Hashtable;

/**
 * Created by paloul on 6/10/16 for data-science
 */
public class JestYelpReview {
    private static String yelpUserFilePath;
    private static String yelpReviewFilePath;

    public static void main(String[] args) throws URISyntaxException, IOException {
        String indexName = "yelp-academic";
        String typeName = "review";
        String awsAddress = "http://search-paloul-ojue2ztt45simkfjvcrv2nrmxm.us-west-2.es.amazonaws.com/";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(awsAddress)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        String path = System.getenv("YELP_ACADEMIC_DATA_REVIEW_PATH");
        if (path != null) yelpReviewFilePath = path;
        else yelpReviewFilePath = "";

        String pathUser = System.getenv("YELP_ACADEMIC_DATA_USER_PATH");
        if (pathUser != null) yelpUserFilePath = pathUser;
        else yelpUserFilePath = "";

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        // Turn path string into java File, and check if exists
        File reviewFile = new File(yelpReviewFilePath);
        if (!reviewFile.exists()) {
            throw new IOException("File Not Found - " + yelpReviewFilePath);
        }
        File userFile = new File(yelpUserFilePath);
        if (!userFile.exists()) {
            throw new IOException("File Not Found - " + yelpUserFilePath);
        }

        // Gson library for sending json to elastic search
        Gson gson = new Gson();
        // Create a timestamp field to add to the json for when we input
        // into the ES index. Business object does not have timestamp field
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        // Read the user file and create a dictionary holding all users and
        // determining if they are elite or not
        Hashtable<String, Boolean> isUserEliteMap = new Hashtable<>();
        try (BufferedReader br = new BufferedReader(new FileReader(userFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject userJson = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj

                String userId = userJson.get("user_id").getAsString();
                boolean isElite = userJson.get("elite").getAsJsonArray().size() > 0;

                isUserEliteMap.put(userId, isElite);
            }
        }

        int reviewBufferCount = 0;
        Collection<JsonObject> reviews = Lists.newArrayList();

        try(BufferedReader br = new BufferedReader(new FileReader(reviewFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject jsonReview = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj

                String userId = jsonReview.get("user_id").getAsString();
                Boolean isElite = isUserEliteMap.get(userId);
                if (isElite != null) {
                    jsonReview.addProperty("is_user_elite", isElite);
                }

                if (reviewBufferCount < 500) {
                    reviews.add(jsonReview);
                    reviewBufferCount++;
                } else {
                    try {
                        Collection<BulkableAction> actions = Lists.newArrayList();
                        reviews.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(gson.toJson(tmp)).build());
                                });
                        Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                        client.execute(bulk.build());
                        reviewBufferCount = 0;
                        reviews = Lists.newArrayList();
                        System.out.println("Inserted 500 documents to cloud");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                Collection<BulkableAction> actions = Lists.newArrayList();
                reviews.stream()
                        .forEach(tmp -> {
                            actions.add(new Index.Builder(gson.toJson(tmp)).build());
                        });
                Bulk.Builder bulk = new Bulk.Builder()
                        .defaultIndex(indexName)
                        .defaultType(typeName)
                        .addAction(actions);
                client.execute(bulk.build());

                System.out.println("Done inserting Reviews to Cloud");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
