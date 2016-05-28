package edu.csula.datascience.ingest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by paloul on 5/25/16 for data-science
 */
public class YelpUser extends ElasticSearchIngest {

    private final String clusterName = "elasticsearch_paloul";
    private final String indexName = "yelp-academic";
    private final String typeName = "user";

    private final String yelpUserFilePath;

    private YelpUser() {
        String path = System.getenv("YELP_ACADEMIC_DATA_USER_PATH");
        if (path != null) this.yelpUserFilePath = path;
        else this.yelpUserFilePath = "";
    }

    void ingest() throws IOException, Exception {
        // Create ES interaction objects
        Node node = nodeBuilder().settings(Settings.builder()
                .put("cluster.name", clusterName)
                .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        // Turn path string into java File, and check if exists
        File tipFile = new File(yelpUserFilePath);
        if (!tipFile.exists()) {
            throw new IOException("File Not Found - " + yelpUserFilePath);
        }

        // Get the bulk processor to submit jobs to es
        BulkProcessor bulkProcessor = getBulkProcessor(client);

        // Gson library for sending json to elastic search
        Gson gson = new Gson();

        // User json object in data files have a date field, named
        // yelping_since, which gives when they were added to yelp
        // We can use that as a timestamp

        try(BufferedReader br = new BufferedReader(new FileReader(tipFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject user = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj

                //System.out.println(user.toString());

                // Add to es bulkprocessor
                bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(user)));
            }
        }
    }

    public static void main(String[] args) {
        YelpUser yelpUser = new YelpUser();
        System.out.println("Ingesting data from file: " + yelpUser.yelpUserFilePath);

        try {
            yelpUser.ingest();
        } catch (IOException e) {
            System.out.println("An error occured reading the data file: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
