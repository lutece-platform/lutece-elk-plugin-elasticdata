package fr.paris.lutece.plugins.elasticdata.service;

import fr.paris.lutece.portal.service.plugin.Plugin;
import fr.paris.lutece.portal.service.plugin.PluginService;
import fr.paris.lutece.portal.service.util.AppPropertiesService;

public class DataSourceUtils
{

    // Build a JSON mappings block to declare 'timestamp' field as a date
    public static final String TIMESTAMP_MAPPINGS = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\" }}}}";
    // Build a JSON mappings block to declare 'timestamp' field as a date and 'location' field as a geo_point
    public static final String TIMESTAMP_AND_LOCATION_MAPPINGS = "{ \"mappings\":  { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\" }, \"location\": { \"type\": \"geo_point\"} } }}}";

    public static final String PLUGIN_NAME = "elasticdata";
    public static final String INSTANCE_NAME = AppPropertiesService.getProperty( "lutece.name" );
    public static final String PREFIX_DATA_OBJECT_ID = INSTANCE_NAME + "_";

    private DataSourceUtils( )
    {
        throw new IllegalStateException( "Utility class" );
    }

    /**
     * Gets the elasticdata plugin
     * 
     * @return the plugin
     */
    public static Plugin getPlugin( )
    {
        return PluginService.getPlugin( PLUGIN_NAME );
    }

}
