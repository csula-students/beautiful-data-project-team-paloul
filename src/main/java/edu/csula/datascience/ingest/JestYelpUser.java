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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

/**
 * Created by paloul on 6/10/16 for data-science
 */
public class JestYelpUser {
    private static String yelpUserFilePath;

    public static void main(String[] args) throws URISyntaxException, IOException, ParseException {
        String indexName = "yelp-academic";
        String typeName = "user";
        String awsAddress = "http://search-paloul-ojue2ztt45simkfjvcrv2nrmxm.us-west-2.es.amazonaws.com/";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(awsAddress)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        String path = System.getenv("YELP_ACADEMIC_DATA_USER_PATH");
        if (path != null) yelpUserFilePath = path;
        else yelpUserFilePath = "";

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        // Turn path string into java File, and check if exists
        File userFile = new File(yelpUserFilePath);
        if (!userFile.exists()) {
            throw new IOException("File Not Found - " + yelpUserFilePath);
        }

        // Gson library for sending json to elastic search
        Gson gson = new Gson();
        // Create a timestamp field to add to the json for when we input
        // into the ES index. Business object does not have timestamp field
        SimpleDateFormat dateFormatParser = new SimpleDateFormat("yyyy-MM");
        SimpleDateFormat dateFormatConverter = new SimpleDateFormat("yyyy-MM-dd");

        int userBufferCount = 0;
        Collection<User> users = Lists.newArrayList();

        try(BufferedReader br = new BufferedReader(new FileReader(userFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject userJson = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj

                //System.out.println(user.get("yelping_since").getAsString());

                String yelping_since = userJson.get("yelping_since").getAsString();
                Date date = dateFormatParser.parse(yelping_since);

                //System.out.println(business.toString());
                User user = new User(
                        userJson.get("type").getAsString(),
                        userJson.get("user_id").getAsString(),
                        userJson.get("name").getAsString(),
                        userJson.get("review_count").getAsInt(),
                        userJson.get("average_stars").getAsDouble(),
                        userJson.get("friends").getAsJsonArray().size(),
                        userJson.get("fans").getAsInt(),
                        userJson.get("elite").getAsJsonArray().size() > 0,
                        dateFormatConverter.format(date)
                );

                if (userBufferCount < 500) {
                    users.add(user);
                    userBufferCount++;
                } else {
                    try {
                        Collection<BulkableAction> actions = Lists.newArrayList();
                        users.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(tmp).build());
                                });
                        Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                        client.execute(bulk.build());
                        userBufferCount = 0;
                        users = Lists.newArrayList();
                        System.out.println("Inserted 500 documents to cloud");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                Collection<BulkableAction> actions = Lists.newArrayList();
                users.stream()
                        .forEach(tmp -> {
                            actions.add(new Index.Builder(tmp).build());
                        });
                Bulk.Builder bulk = new Bulk.Builder()
                        .defaultIndex(indexName)
                        .defaultType(typeName)
                        .addAction(actions);
                client.execute(bulk.build());

                System.out.println("Done inserting Businesses to Cloud");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}

class User {
    private final String type;
    private final String userId;
    private final String name;
    private final int reviewCount;
    private final double averageStars;
    private final int numberFriends;
    private final int numberFans;
    private final Boolean isElite;
    private final String date;

    public User(
            String type,
            String userId,
            String name,
            int reviewCount,
            double averageStars,
            int numberFriends,
            int numberFans,
            Boolean isElite,
            String date) {

        this.type = type;
        this.userId = userId;
        this.name = name;
        this.reviewCount = reviewCount;
        this.averageStars = averageStars;
        this.numberFriends = numberFriends;
        this.numberFans = numberFans;
        this.isElite = isElite;
        this.date = date;
    }
}