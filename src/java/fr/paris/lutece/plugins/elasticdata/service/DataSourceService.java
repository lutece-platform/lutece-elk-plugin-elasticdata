/*
 * Copyright (c) 2002-2017, Mairie de Paris
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice
 *     and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice
 *     and the following disclaimer in the documentation and/or other materials
 *     provided with the distribution.
 *
 *  3. Neither the name of 'Mairie de Paris' nor 'Lutece' nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * License 1.0
 */
package fr.paris.lutece.plugins.elasticdata.service;

import fr.paris.lutece.plugins.elasticdata.business.DataObject;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.spring.SpringContextService;
import fr.paris.lutece.portal.service.util.AppPropertiesService;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * DataSourceService
 */
public final class DataSourceService
{
    private static final String PROPERTY_ELASTIC_SERVER_URL = "elasticdata.elastic_server.url";
    private static final String DEFAULT_ELASTIC_SERVER_URL = "httt://localhost:9200";

    private static Map<String, DataSource> _mapDataSources;

    /** Package constructor */
    DataSourceService()
    {
    }

    /**
     * Gets all data sources found into Spring context files
     * 
     * @return The list
     */
    public static Collection<DataSource> getDataSources( )
    {
        synchronized( DataSourceService.class ) 
        {
            if ( _mapDataSources == null )
            {
                _mapDataSources = new HashMap( );
                for ( DataSource source : SpringContextService.getBeansOfType( DataSource.class ) )
                {
                    _mapDataSources.put( source.getId( ), source );
                }
            }
        }
        return _mapDataSources.values( );
    }

    /**
     * Get a Data Source from its ID
     * 
     * @param strId
     *            The ID
     * @return The Data Source
     */
    public static DataSource getDataSource( String strId )
    {
        return _mapDataSources.get( strId );
    }

    /**
     * Insert data from a DataSource into Elastic Search
     * 
     * @param sbLogs
     *            A log buffer
     * @param dataSource
     *            The data source
     * @param bReset
     *            if the index should be reset before inserting
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    public static void insertData( StringBuilder sbLogs, DataSource dataSource, boolean bReset ) throws ElasticClientException
    {
        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );
        if ( bReset )
        {
            elastic.deleteIndex( dataSource.getTargetIndexName( ) );
            elastic.createMappings( dataSource.getTargetIndexName( ), getTimestampMappings( dataSource.getDataType( ) ) );
        }
        Collection<DataObject> listDataObjects = dataSource.getDataObjects( );
        for ( DataObject object : listDataObjects )
        {
            elastic.create( dataSource.getTargetIndexName( ), dataSource.getDataType( ), object );
        }
        sbLogs.append( "Number of object inserted for Data Source '" ).append( dataSource.getName( ) ).append( "' : " ).append( listDataObjects.size( ) );
    }

    /**
     * Insert All data sources
     * 
     * @param bReset
     *            Reset the index before inserting
     * @return The logs of the process
     * @throws ElasticClientException If an error occurs accessing to ElasticSearch
     */
    public static String insertDataAllDatasources( boolean bReset ) throws ElasticClientException
    {
        StringBuilder sbLogs = new StringBuilder( );

        for ( DataSource dataSource : getDataSources( ) )
        {
            insertData( sbLogs, dataSource, bReset );
        }
        return sbLogs.toString( );
    }

    /**
     * Build a JSON mappings block to declare 'timestamp' field as a date
     * 
     * @param strType
     *            The document type
     * @return The JSON
     */
    private static String getTimestampMappings( String strType )
    {
        return "{ \"mappings\": { \"" + strType + "\" : { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"epoch_millis\" }}}}}";
    }
}
