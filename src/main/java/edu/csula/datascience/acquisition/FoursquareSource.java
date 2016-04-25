package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;
import fi.foyt.foursquare.api.entities.Category;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import fi.foyt.foursquare.api.FoursquareApi;
import fi.foyt.foursquare.api.FoursquareApiException;
import fi.foyt.foursquare.api.Result;
import fi.foyt.foursquare.api.entities.CompactVenue;
import fi.foyt.foursquare.api.entities.VenuesSearchResult;

import java.util.Collection;
import java.util.List;

/**
 * An example of Source implementation using Twitter4j api to grab tweets
 */
public class FoursquareSource implements Source<CompactVenue> {

    private final int return_limit = 50; // Limit set by Foursquare

    private final String client_id;
    private final String client_secret;

    private final JSONArray cities;
    private final int cities_count;
    private int current_city_index;

    public FoursquareSource(JSONArray cities) {
        this.client_id = System.getenv("FOURSQUARE_CLIENT_ID");
        this.client_secret = System.getenv("FOURSQUARE_CLIENT_SECRET");

        this.cities = cities;
        this.cities_count = this.cities.size();

        this.current_city_index = 0;
    }

    @Override
    public boolean hasNext() {
        return this.current_city_index < this.cities_count;
    }

    @Override
    public Collection<CompactVenue> next() {
        List<CompactVenue> list = Lists.newArrayList();

        String near_city = (String) this.cities.get(this.current_city_index++);

        FoursquareApi foursquareApi = new FoursquareApi(this.client_id, this.client_secret, "www.paloulmedia.com");

        // After client has been initialized we can make queries.
        Result<VenuesSearchResult> result = null;
        try {
            result = foursquareApi.venuesSearch(
                    near_city, null, return_limit, null, null, null, null, null);

            if (result.getMeta().getCode() == 200) {
                // if query was ok we can finally we do something with the data
                for (CompactVenue venue : result.getResult().getVenues()) {
                    list.add(venue);
                }

                return list;

            } else {
                // TODO: Proper error handling
                System.out.println("Error occured: ");
                System.out.println("  code: " + result.getMeta().getCode());
                System.out.println("  type: " + result.getMeta().getErrorType());
                System.out.println("  detail: " + result.getMeta().getErrorDetail());
            }

        } catch (FoursquareApiException e) {
            e.printStackTrace();
        }

        return Lists.newArrayList();
    }

    public void showEnvVariables() {
        System.out.println("FOURSQUARE_CLIENT_ID: " + System.getenv("FOURSQUARE_CLIENT_ID"));
        System.out.println("FOURSQUARE_CLIENT_SECRET: " + System.getenv("FOURSQUARE_CLIENT_SECRET"));
    }
}
