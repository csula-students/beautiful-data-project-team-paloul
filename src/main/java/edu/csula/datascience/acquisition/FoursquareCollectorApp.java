package edu.csula.datascience.acquisition;



import fi.foyt.foursquare.api.entities.CompactVenue;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

/**
 * A simple example of using Twitter
 */
public class FoursquareCollectorApp {
    public static void main(String[] args) {

        File f;
        FileReader reader = null;

        try {
            // Use the neighborhoods.json file to get all neighborhoods Yelp provides content for

            f = new File(YelpBusinessSource.class.getResource("US_top_1000_cities.json").toURI());

            reader = new FileReader(f);

            JSONParser parser = new JSONParser();
            JSONObject parsed_obj = (JSONObject) parser.parse(reader);

            JSONArray cities_array = (JSONArray) parsed_obj.get("cities");

            FoursquareSource source = new FoursquareSource(cities_array);
            FoursquareCollector collector = new FoursquareCollector();

            while (source.hasNext()) {
                Collection<CompactVenue> venues = source.next();
                Collection<JSONObject> cleanedVenues = collector.mungee(venues);

                collector.save(cleanedVenues);
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
