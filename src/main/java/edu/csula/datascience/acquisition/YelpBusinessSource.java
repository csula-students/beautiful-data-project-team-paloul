package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Collection;
import java.util.List;

/**
 * An example of Source implementation using Twitter4j api to grab tweets
 */
public class YelpBusinessSource implements Source<JSONObject> {
    private final int sort;
    private final int limit;
    private final int offset;
    private final int radius;
    private final String location;
    private final int offset_maximum;

    private int curr_offset;
    private long total_found;

    private YelpAPI yelpAPI;

    public YelpBusinessSource(String location, int limit, int offset, int offset_maximum, int radius, int sort) {
        this.sort = sort;
        this.limit = limit;
        this.offset = offset;
        this.radius = radius;
        this.location = location;
        this.offset_maximum = offset_maximum;

        this.curr_offset = this.offset;
        this.total_found = Integer.MAX_VALUE;

        this.yelpAPI = new YelpAPI(
                System.getenv("YELP_CONSUMER_KEY"),
                System.getenv("YELP_CONSUMER_SECRET"),
                System.getenv("YELP_TOKEN"),
                System.getenv("YELP_TOKEN_SECRET")
        );
    }

    public void showEnvVariables() {
        System.out.println("YELP_CONSUMER_KEY: " + System.getenv("YELP_CONSUMER_KEY"));
        System.out.println("YELP_CONSUMER_SECRET: " + System.getenv("YELP_CONSUMER_SECRET"));
        System.out.println("YELP_TOKEN: " + System.getenv("YELP_TOKEN"));
        System.out.println("YELP_TOKEN_SECRET: " + System.getenv("YELP_TOKEN_SECRET"));
    }

    @Override
    public boolean hasNext() {
        // There is a maximum offset (1000) imposed by Yelp :(
        // Curr_Offset should be less than max allowed offset by yelp and
        // less than the total found businesses for this location
        return (this.curr_offset < this.offset_maximum && this.curr_offset < this.total_found);
    }

    @Override
    public Collection<JSONObject> next() {
        String searchResponseJSON =
                this.yelpAPI.searchForBusinessesByLocation(
                        this.location, this.limit, this.curr_offset, this.sort, this.radius);

        List<JSONObject> list = Lists.newArrayList();

        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            response = (JSONObject) parser.parse(searchResponseJSON);
        } catch (ParseException pe) {
            System.out.println("Error: could not parse JSON response:");
            System.out.println(searchResponseJSON);
            System.exit(1);
        }

        if (response != null && !response.containsKey("error")) {
            this.total_found = (long) response.get("total");
            System.out.println("Total Number of Businesses Found: " + total_found);

            JSONArray businesses = (JSONArray) response.get("businesses");
            if (businesses != null && businesses.size() > 0) {
                for (int i = 0; i < businesses.size(); i++) {
                    JSONObject business = (JSONObject) businesses.get(i);
                    list.add(business);
                }

                this.curr_offset += businesses.size();

                return list;
            }
        }

        this.total_found = Integer.MIN_VALUE;
        return Lists.newArrayList(); // return empty list
    }

}
