package fr.paris.lutece.plugins.elasticdata.service;

public class DataSourceUtils {
	
	//Build a JSON mappings block to declare 'timestamp' field as a date
	public static final  String TIMESTAMP_MAPPINGS="{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\" }}}}";
	// Build a JSON mappings block to declare 'timestamp' field as a date and 'location' field as a geo_point
	public static final  String TIMESTAMP_AND_LOCATION_MAPPINGS="{ \"mappings\":  { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\" }, \"location\": { \"type\": \"geo_point\"} } }}}";
    
	private DataSourceUtils() {
	    throw new IllegalStateException("Utility class");
	}

}
