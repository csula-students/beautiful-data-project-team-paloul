package edu.csula.datascience.acquisition;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;

/**
 * A simple example of using Twitter
 */
public class YelpBusinessCollectorApp {

    private static final int OFFSET_MAXIMUM = 1000; // Limit imposed by YELP API
    private static final int SORT_TYPE = 0; // 0=BestMatched, 1=Distance, 2=HighestRated
    private static final int SEARCH_LIMIT = 20; // Max=20 limited by Yelp
    private static final int STARTING_OFFSET = 0; // Starting offset of returned values
    private static final int RADIUS_METERS = 5000; // The radius boundary surrounding given location

    public static void main(String[] args) {

        File f;
        FileReader reader = null;

        try {
            // Use the neighborhoods.json file to get all neighborhoods Yelp provides content for

            f = new File(YelpBusinessSource.class.getResource("neighborhoods.json").toURI());

            reader = new FileReader(f);

            JSONParser parser = new JSONParser();
            JSONObject neighborhoods_obj = (JSONObject) parser.parse(reader);

            JSONArray neighborhood_array = (JSONArray) neighborhoods_obj.get("neighborhoods");

            YelpBusinessCollector collector = new YelpBusinessCollector();

            //System.out.println(neighborhood_array.toJSONString());

            // Optional: Fast Forward to last known neighborhood we were downloading
            int start_from = 0;
//            String start_neighborhood = (String) neighborhood_array.get(start_from);
//            while(!start_neighborhood.equals("Queen Street West")) {
//                start_neighborhood = (String) neighborhood_array.get(start_from++);
//            }

            for (int i = start_from; i < neighborhood_array.size(); i++) {
                String neighborhood = (String) neighborhood_array.get(i);

                //System.out.println(neighborhood);

                YelpBusinessSource source = new YelpBusinessSource(
                        neighborhood, SEARCH_LIMIT, STARTING_OFFSET, OFFSET_MAXIMUM, RADIUS_METERS, SORT_TYPE);

                while (source.hasNext()) {
                    Collection<JSONObject> businesses = source.next();
                    Collection<JSONObject> cleaned_businesses = collector.mungee(businesses);
                    collector.save(cleaned_businesses);
                }
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // This is unrecoverable. Just report it and move on
                    e.printStackTrace();
                }
            }
        }
    }
}
