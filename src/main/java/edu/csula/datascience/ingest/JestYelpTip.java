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

/**
 * Created by paloul on 6/10/16 for data-science
 */
public class JestYelpTip {
    private static String yelpTipilePath;

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

        String path = System.getenv("YELP_ACADEMIC_DATA_TIP_PATH");
        if (path != null) yelpTipilePath = path;
        else yelpTipilePath = "";

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        // Turn path string into java File, and check if exists
        File tipFile = new File(yelpTipilePath);
        if (!tipFile.exists()) {
            throw new IOException("File Not Found - " + yelpTipilePath);
        }

        // Gson library for sending json to elastic search
        Gson gson = new Gson();
        // Create a timestamp field to add to the json for when we input
        // into the ES index. Business object does not have timestamp field
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        int tipBufferCount = 0;
        Collection<JsonObject> tips = Lists.newArrayList();

        try(BufferedReader br = new BufferedReader(new FileReader(tipFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject jsonCheckin = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj

                if (tipBufferCount < 500) {
                    tips.add(jsonCheckin);
                    tipBufferCount++;
                } else {
                    try {
                        Collection<BulkableAction> actions = Lists.newArrayList();
                        tips.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(gson.toJson(tmp)).build());
                                });
                        Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                        client.execute(bulk.build());
                        tipBufferCount = 0;
                        tips = Lists.newArrayList();
                        System.out.println("Inserted 500 documents to cloud");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                Collection<BulkableAction> actions = Lists.newArrayList();
                tips.stream()
                        .forEach(tmp -> {
                            actions.add(new Index.Builder(gson.toJson(tmp)).build());
                        });
                Bulk.Builder bulk = new Bulk.Builder()
                        .defaultIndex(indexName)
                        .defaultType(typeName)
                        .addAction(actions);
                client.execute(bulk.build());

                System.out.println("Done inserting Tips to Cloud");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
