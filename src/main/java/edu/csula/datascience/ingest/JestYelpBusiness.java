package edu.csula.datascience.ingest;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.csula.datascience.examples.JestExampleApp;
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

/**
 * Created by paloul on 6/10/16 for data-science
 */
public class JestYelpBusiness {

    private static String yelpBusinessFilePath;

    public static void main(String[] args) throws URISyntaxException, IOException {
        String indexName = "yelp-academic";
        String typeName = "business";
        String awsAddress = "http://search-paloul-ojue2ztt45simkfjvcrv2nrmxm.us-west-2.es.amazonaws.com/";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(awsAddress)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        String path = System.getenv("YELP_ACADEMIC_DATA_BUSINESS_PATH");
        if (path != null) yelpBusinessFilePath = path;
        else yelpBusinessFilePath = "";

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        // Turn path string into java File, and check if exists
        File businessFile = new File(yelpBusinessFilePath);
        if (!businessFile.exists()) {
            throw new IOException("File Not Found - " + yelpBusinessFilePath);
        }

        // Gson library for sending json to elastic search
        Gson gson = new Gson();
        // Create a timestamp field to add to the json for when we input
        // into the ES index. Business object does not have timestamp field
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        int businessBufferCount = 0;
        Collection<Business> businesses = Lists.newArrayList();

        try(BufferedReader br = new BufferedReader(new FileReader(businessFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject jsonBiz = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj

                //System.out.println(business.toString());
                Business biz = new Business(
                        jsonBiz.get("type").getAsString(),
                        jsonBiz.get("business_id").getAsString(),
                        jsonBiz.get("name").getAsString(),
                        jsonBiz.get("city").getAsString(),
                        jsonBiz.get("state").getAsString(),
                        jsonBiz.get("stars").getAsDouble(),
                        jsonBiz.get("review_count").getAsInt()
                );

                if (businessBufferCount < 500) {
                    businesses.add(biz);
                    businessBufferCount++;
                } else {
                    try {
                        Collection<BulkableAction> actions = Lists.newArrayList();
                        businesses.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(tmp).build());
                                });
                        Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                        client.execute(bulk.build());
                        businessBufferCount = 0;
                        businesses = Lists.newArrayList();
                        System.out.println("Inserted 500 documents to cloud");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                Collection<BulkableAction> actions = Lists.newArrayList();
                businesses.stream()
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

class Business {
    final String type;
    final String businessId;
    final String businessName;
    final String city;
    final String state;
    final double stars;
    final int reviewCount;

    public Business(
            String type,
            String businessId,
            String businessName,
            String city,
            String state,
            double stars,
            int reviewCount) {

        this.type = type;
        this.businessId = businessId;
        this.businessName = businessName;
        this.city = city;
        this.state = state;
        this.stars = stars;
        this.reviewCount = reviewCount;
    }
}