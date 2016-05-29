package edu.csula.datascience.ingest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by paloul on 5/25/16 for data-science
 */
public class YelpCheckin extends ElasticSearchIngest {

    private final String clusterName = "elasticsearch_paloul";
    private final String indexName = "yelp-academic";
    private final String typeName = "checkin";

    private final String yelpCheckinFilePath;

    private YelpCheckin() {

        String path = System.getenv("YELP_ACADEMIC_DATA_CHECKIN_PATH");
        if (path != null) this.yelpCheckinFilePath = path;
        else this.yelpCheckinFilePath = "";
    }

    void ingest() throws Exception {
        // Create ES interaction objects
        Node node = nodeBuilder().settings(Settings.builder()
                .put("cluster.name", clusterName)
                .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        // Turn path string into java File, and check if exists
        File checkinFile = new File(yelpCheckinFilePath);
        if (!checkinFile.exists()) {
            throw new IOException("File Not Found - " + yelpCheckinFilePath);
        }

        // Get the bulk processor to submit jobs to es
        BulkProcessor bulkProcessor = getBulkProcessor(client);

        // Gson library for sending json to elastic search
        Gson gson = new Gson();

        // Create a timestamp field to add to the json for when we input
        // into the ES index. Checkin object does not have timestamp field
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try(BufferedReader br = new BufferedReader(new FileReader(checkinFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject checkin = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj
                checkin.addProperty("created", dateFormat.format(new Date())); // Add a timestamp to business json obj

                //System.out.println(checkin.toString());

                // Add to es bulkprocessor
                bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(checkin)));
            }
        }
    }

    public static void main(String[] args) {
        YelpCheckin yelpCheckin = new YelpCheckin();
        System.out.println("Ingesting data from file: " + yelpCheckin.yelpCheckinFilePath);

        try {
            yelpCheckin.ingest();
        } catch (IOException e) {
            System.out.println("An error occured reading the data file: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
