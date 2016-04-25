package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;
import fi.foyt.foursquare.api.entities.CompactVenue;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

/**
 * A test case to show how to use Collector and Source
 */
public class FoursquareCollectorTest {
    private FoursquareCollector collector;
    private FoursquareSource source;

    @Before
    public void setup() {
        JSONArray cities = new JSONArray();
        cities.add("Los Angeles, CA");
        cities.add("San Diego, CA");
        cities.add("San Francisco, CA");

        collector = new FoursquareCollector();
        source = new FoursquareSource(cities);
    }

    @Test
    public void mungee() throws Exception {

        int count = 0;
        while (source.hasNext()) {
            Collection<CompactVenue> venues = source.next();
            Collection<JSONObject> cleanedVenues = collector.mungee(venues);

            // Verify cleanedVenues returns less than or equal to 50 venues, never more than 50 (Foursquare limitation)
            Assert.assertTrue(cleanedVenues.size() <= 50);

            count++;
        }

        // hasNext is based off how many cities are given
        // Foursquare API returns max 50 per city request
        // source.next executes n cities count
        Assert.assertEquals(count, 3);
    }
}