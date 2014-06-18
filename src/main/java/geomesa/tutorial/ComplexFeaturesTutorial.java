package geomesa.tutorial;

import geomesa.core.data.AccumuloFeatureStore;
import geomesa.tutorial.GdeltFeature.Attributes;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.geotools.data.DataAccess;
import org.geotools.data.DataAccessFinder;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.data.complex.config.EmfAppSchemaReader;
import org.geotools.data.complex.filter.XPathUtil;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.NameImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.gml3.GML;
import org.geotools.gml3.GMLConfiguration;
import org.geotools.xml.AppSchemaConfiguration;
import org.geotools.xml.AppSchemaLocationResolver;
import org.geotools.xml.Configuration;
import org.geotools.xml.Encoder;
import org.geotools.xml.resolver.SchemaResolver;
import org.opengis.feature.ComplexAttribute;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright 2014 Commonwealth Computer Research, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ComplexFeaturesTutorial {

    public static final String GSML_NAMESPACE = "urn:cgi:xmlns:CGI:GeoSciML:2.0";

    public static final QName type = new QName(GSML_NAMESPACE, "MappedFeature");

    public static final Name nameProperty = new NameImpl(GML.name);

    public static final Name shapeProperty = new NameImpl(new QName(GSML_NAMESPACE, "shape"));

    public static final Name dateProperty = new NameImpl(GML.CalDate);

    /**
     * Creates a base filter that will return a small subset of our results. This can be tweaked to
     * return different results if desired. Currently it should return 16 results.
     *
     * @return
     *
     * @throws CQLException
     * @throws IOException
     */
    static Filter createBaseFilter()
            throws CQLException, IOException {

        // Get a FilterFactory2 to build up our query
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

        // We are going to query for events in Ukraine during the
        // civil unrest.

        // We'll start by looking at a particular day in February of 2014
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, 2013);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        Date start = calendar.getTime();

        calendar.set(Calendar.YEAR, 2014);
        calendar.set(Calendar.MONTH, Calendar.APRIL);
        calendar.set(Calendar.DAY_OF_MONTH, 30);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        Date end = calendar.getTime();
//        2013-01-01T00:00:00.000Z/2014-04-30T23:00:00.000Z
        Filter timeFilter =
                ff.between(ff.property(dateProperty),
                           ff.literal(start),
                           ff.literal(end));

        // We'll bound our query spatially to Ukraine
        Filter spatialFilter =
                ff.bbox(ff.property("gsml:" + shapeProperty.getLocalPart()),
//                        31.6, 44, 37.4, 47.75,
                        31.6, 44, 31.61, 44.01,
                        "EPSG:4326");

        // we'll also restrict our query to only articles about the US, UK or UN
        Filter attributeFilter = ff.like(ff.property("gml:" + nameProperty.getLocalPart()), "UNITED%");

        // Now we can combine our filters using a boolean AND operator
        Filter conjunction = ff.and(Arrays.asList(/*timeFilter, */spatialFilter, attributeFilter));

//        SchemaResolver resolver = new SchemaResolver();
//        AppSchemaConfiguration configuration = new AppSchemaConfiguration(
//                "urn:cgi:xmlns:CGI:GeoSciML:2.0",
//                "http://www.geosciml.org/geosciml/2.0/xsd/geosciml.xsd",
//                resolver);
//        // add a GML Configuration
//        configuration.addDependency(new GMLConfiguration());
//
//        Encoder encoder = new Encoder(configuration);
//        encoder.encode(conjunction, type);

        return conjunction;
    }

    /**
     * Executes a basic bounding box query
     *
     * @param simpleFeatureTypeName
     * @param dataStore
     *
     * @throws IOException
     * @throws CQLException
     */
    static void executeQuery(String simpleFeatureTypeName, DataStore dataStore)
            throws IOException, CQLException {

        // start with our basic filter to narrow the results
        Filter cqlFilter = createBaseFilter();

        // use the 2-arg constructor for the query - this will not restrict the attributes returned
        Query query = new Query(simpleFeatureTypeName, cqlFilter);

        // get the feature store used to query the GeoMesa data
        FeatureStore featureStore = (AccumuloFeatureStore) dataStore.getFeatureSource(
                simpleFeatureTypeName);

        // execute the query
        FeatureCollection results = featureStore.getFeatures(query);

        // loop through all results
        FeatureIterator iterator = results.features();
        try {
            printResults(iterator, Attributes.geom.getName());
        } finally {
            iterator.close();
        }
    }

    /**
     * Iterates through the given iterator and prints out the properties (attributes) for each entry.
     *
     * @param iterator
     */
    private static void printResults(FeatureIterator iterator, String... attributes) {
        if (iterator.hasNext()) {
            System.out.println("Results:");
        } else {
            System.out.println("No results");
        }
        int n = 0;
        while (iterator.hasNext()) {
            Feature feature = iterator.next();
            StringBuilder result = new StringBuilder();
            result.append(++n);

            for (String attribute : attributes) {
                Property property = feature.getProperty(attribute);
                result.append("|")
                      .append(property.getName())
                      .append('=')
                      .append(property.getValue());
            }
            System.out.println(result.toString());
        }
        System.out.println();
    }

    /**
     * Main entry point. Executes queries against an existing GDELT dataset.
     *
     * @param args
     *
     * @throws Exception
     */
    public static void main(String[] args)
            throws Exception {
        // read command line options - this contains the connection to accumulo and the table to query
//        CommandLineParser parser = new BasicParser();
//        Options options = SetupUtil.getGeomesaDataStoreOptions();
//        CommandLine cmd = parser.parse(options, args);

        URL mappingFile = ComplexFeaturesTutorial.class.getClassLoader().getResource(
                "gdelt-gsm-mapping.xml");

        System.out.println("Using mapping file: " + mappingFile);

        // verify that we can see this Accumulo destination in a GeoTools manner
        Map<String, Serializable> configuration = new HashMap<String, Serializable>();
        configuration.put("dbtype", "app-schema");
        configuration.put("url", mappingFile.toURI().toString());

        DataAccess<FeatureType, Feature> dataAccess = null;
        try {
            dataAccess = DataAccessFinder.getDataStore(configuration);
            FeatureSource<FeatureType, Feature> source = dataAccess.getFeatureSource(new NameImpl(
                    type));
System.out.println("got feature source");
            Filter cqlFilter = createBaseFilter();

            // use the 2-arg constructor for the query - this will not restrict the attributes returned
            Query query = new Query(type.getLocalPart(), cqlFilter);
            query.setNamespace(URI.create(type.getNamespaceURI()));
            query.setMaxFeatures(10);

            FeatureCollection<FeatureType, Feature> features = source.getFeatures(query);
            FeatureIterator<Feature> iterator = features.features();
            try {
                while (iterator.hasNext()) {
                    Feature f = iterator.next();
                    System.out.println("Feature "
                                       + f.getIdentifier().toString()
                                       + " has gml:name = "
                                       + ((ComplexAttribute) f.getProperty(nameProperty))
                            .getProperty("simpleContent").getValue());
                    for (Property property : f.getProperties()) {
                        System.out.println(property.getName() + " " + property.getValue());
                    }
                }
            } finally {
                iterator.close();
            }
        } finally {
            if (dataAccess != null) {
                dataAccess.dispose();
            }
        }
    }


}