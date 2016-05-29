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
public class YelpBusiness extends ElasticSearchIngest {

    private final String pathHome = "elasticsearch_data";
    private final String clusterName = "elasticsearch_paloul";
    private final String indexName = "yelp-academic";
    private final String typeName = "business";

    private final String yelpBusinessFilePath;

    private YelpBusiness() {
        String path = System.getenv("YELP_ACADEMIC_DATA_BUSINESS_PATH");
        if (path != null) this.yelpBusinessFilePath = path;
        else this.yelpBusinessFilePath = "";
    }

    void ingest() throws IOException, Exception {
        // Create ES interaction objects
        Node node = nodeBuilder().settings(Settings.builder()
                .put("cluster.name", clusterName)
                .put("path.home", pathHome)).node();
        Client client = node.client();

        // Turn path string into java File, and check if exists
        File businessFile = new File(yelpBusinessFilePath);
        if (!businessFile.exists()) {
            throw new IOException("File Not Found - " + yelpBusinessFilePath);
        }

        // Get the bulk processor to submit jobs to es
        BulkProcessor bulkProcessor = getBulkProcessor(client);

        // Gson library for sending json to elastic search
        Gson gson = new Gson();
        // Create a timestamp field to add to the json for when we input
        // into the ES index. Business object does not have timestamp field
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try(BufferedReader br = new BufferedReader(new FileReader(businessFile))) {
            for(String line; (line = br.readLine()) != null; ) {

                JsonObject business = gson.fromJson(line, JsonObject.class); // Convert json from file to business json obj
                business.addProperty("created", dateFormat.format(new Date())); // Add a timestamp to business json obj

                //System.out.println(business.toString());

                // Add to es bulkprocessor
                bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(business)));
            }
        }

        client.close();
        node.close();
    }

    public static void main(String[] args) {
        YelpBusiness yelpBusiness = new YelpBusiness();
        System.out.println("Ingesting data from file: " + yelpBusiness.yelpBusinessFilePath);

        try {
            yelpBusiness.ingest();
        } catch (IOException e) {
            System.out.println("An error occured reading the data file: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
