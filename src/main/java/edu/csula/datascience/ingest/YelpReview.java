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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by paloul on 5/25/16 for data-science
 */
public class YelpReview extends ElasticSearchIngest {

    private final String clusterName = "elasticsearch_paloul";
    private final String indexName = "yelp-academic";
    private final String typeName = "review";

    private final String yelpReviewFilePath;

    private YelpReview() {
        String path = System.getenv("YELP_ACADEMIC_DATA_REVIEW_PATH");
        if (path != null) this.yelpReviewFilePath = path;
        else this.yelpReviewFilePath = "";
    }

    void ingest() throws IOException, Exception {
        // Create ES interaction objects
        Node node = nodeBuilder().settings(Settings.builder()
                .put("cluster.name", clusterName)
                .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        // Turn path string into java File, and check if exists
        File reviewFile = new File(yelpReviewFilePath);
        if (!reviewFile.exists()) {
            throw new IOException("File Not Found - " + yelpReviewFilePath);
        }

        // Get the bulk processor to submit jobs to es
        BulkProcessor bulkProcessor = getBulkProcessor(client);

        // Gson library for sending json to elastic search
        Gson gson = new Gson();

        // Review json object in data files have a date field.
        // No need to create a separate timestamp for ES

        try(BufferedReader br = new BufferedReader(new FileReader(reviewFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject review = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj

                //System.out.println(review.toString());

                // Add to es bulkprocessor
                bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(review)));
            }
        }
    }

    public static void main(String[] args) {
        YelpReview yelpReview = new YelpReview();
        System.out.println("Ingesting data from file: " + yelpReview.yelpReviewFilePath);

        try {
            yelpReview.ingest();
        } catch (IOException e) {
            System.out.println("An error occured reading the data file: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
