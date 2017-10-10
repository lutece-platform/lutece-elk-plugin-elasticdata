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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import fr.paris.lutece.plugins.elasticdata.business.DataObject;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.libraryelastic.business.bulk.BulkRequest;
import fr.paris.lutece.plugins.libraryelastic.business.bulk.IndexSubRequest;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.spring.SpringContextService;
import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.portal.service.util.AppPropertiesService;

/**
 * DataSourceService
 */
public final class DataSourceService
{
    private static final String PROPERTY_ELASTIC_SERVER_URL = "elasticdata.elastic_server.url";
    private static final String DEFAULT_ELASTIC_SERVER_URL = "httt://localhost:9200";
    private static final String PROPERTY_BULK_BATCH_SIZE = "elasticdata.bulk_batch_size";
    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final int BATCH_SIZE = AppPropertiesService.getPropertyInt( PROPERTY_BULK_BATCH_SIZE, DEFAULT_BATCH_SIZE );
            
    private static Map<String, DataSource> _mapDataSources;

    /** Package constructor */
    DataSourceService( )
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
        long timeBegin = System.currentTimeMillis();
        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );
        if ( bReset )
        {
            if ( elastic.isExists( dataSource.getTargetIndexName( ) ) )
            {
                elastic.deleteIndex( dataSource.getTargetIndexName( ) );
            }
            elastic.createMappings( dataSource.getTargetIndexName( ), getMappings( dataSource ));
        }
        
        int nBatchSize = ( dataSource.getBatchSize() != 0 ) ? dataSource.getBatchSize() : BATCH_SIZE;
        int nbDocsInsert=insertObjects( elastic , dataSource, dataSource.getDataObjectsIterator() , nBatchSize );
       
        long timeEnd = System.currentTimeMillis();
        sbLogs.append( "Number of object inserted for Data Source '" ).append( dataSource.getName( ) ).append( "' : " ).append( nbDocsInsert );
        sbLogs.append( " (duration : " ).append( timeEnd - timeBegin ).append( "ms)\n");
    }
    
    /**
     * Insert a list of object in bulk mode  
     * @param elastic The Elastic Server
     * @param dataSource The data source
     * @param listDataObjects The list of objects
     * @param nBatchSize The number of objects in each bulk request
     * @throws ElasticClientException  If a problem occurs connecting the server
     * @return the number of documents posted 
     */
    static int insertObjects( Elastic elastic, DataSource dataSource, Iterator<DataObject> iterateDataObjects , int nBatchSize ) throws ElasticClientException
    {
        List<DataObject> listBatch = new ArrayList<DataObject>();

       
        int nCount = 0;
      
        while( iterateDataObjects.hasNext() )
        {
            DataObject object = iterateDataObjects.next();
            nCount++;
            listBatch.add( object );
            if( ( listBatch.size() == nBatchSize ) || !iterateDataObjects.hasNext() )
            {
                BulkRequest br = new BulkRequest();
                for( Object batchObject : listBatch )
                {
                    IndexSubRequest isr = new IndexSubRequest( null );
                    br.addAction( isr, batchObject );
                }
                AppLogService.info( "ElasticData indexing : Posting bulk action for " + listBatch.size() + " documents of DataSource '" + dataSource.getName() + "'" );
                insertBulkData( elastic, dataSource, br );
                listBatch.clear();
            }
           
        }
        AppLogService.info( "ElasticData indexing : completed for " + nCount + " documents of DataSource '" + dataSource.getName() + "'" );
       
        return nCount;
    }
    
    /**
     * Insert one dataObject from a DataSource into Elastic Search
     * 
     * @param elastic
     *            The elasticserver, can be null
     * @param dataSource
     *            The data source
     * @param dataObject
     *            The data object
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    private static void insertData( Elastic elastic, DataSource dataSource, DataObject dataObject ) throws ElasticClientException
    {
        if ( elastic == null )
        {
            String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
            elastic = new Elastic( strServerUrl );
        }
        elastic.create( dataSource.getTargetIndexName( ), dataSource.getDataType( ), dataObject );
    }    
        
    /**
     * Insert one dataObject from a DataSource into Elastic Search
     * 
     * @param elastic
     *            The elasticserver, can be null
     * @param dataSource
     *            The data source
     * @param dataObject
     *            The data object
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    private static void insertBulkData( Elastic elastic, DataSource dataSource, BulkRequest bulkRequest ) throws ElasticClientException
    {
        if ( elastic == null )
        {
            String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
            elastic = new Elastic( strServerUrl );
        }
        String strResponse = elastic.createByBulk( dataSource.getTargetIndexName( ), dataSource.getDataType( ), bulkRequest );
        AppLogService.debug( "ElasticData : Response of the posted bulk request : " + strResponse );
        
    }

    /**
     * Insert one dataObject from a DataSource into Elastic Search
     * 
     * @param dataSource
     *            The data source
     * @param dataObject
     *            The data object
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    public static void insertData( DataSource dataSource, DataObject dataObject ) throws ElasticClientException
    {
        insertData( null, dataSource, dataObject );
    }

    /**
     * Insert All data sources
     * 
     * @param bReset
     *            Reset the index before inserting
     * @return The logs of the process
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    public static String insertDataAllDatasources( boolean bReset ) throws ElasticClientException
    {
        return insertDataAllDatasources( bReset , false );
    }
    
    /**
     * Insert All data sources
     * 
     * @param bReset
     *            Reset the index before inserting
     * @param bDaemon
     *            If called by a daemon
     * @return The logs of the process
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    public static String insertDataAllDatasources( boolean bReset , boolean bDaemon ) throws ElasticClientException
    {
        StringBuilder sbLogs = new StringBuilder( );

        for ( DataSource dataSource : getDataSources( ) )
        {
            if( dataSource.usesFullIndexingDaemon() || !bDaemon )
            {
                insertData( sbLogs, dataSource, bReset );
            }
         }
        return sbLogs.toString( );
    }

    /**
     * Return the mappings associated to a data source
     * @param dataSource The data source
     * @return The mappings string as JSON
     */
    private static String getMappings( DataSource dataSource )
    {
        if( dataSource.getMappings() != null )
        {
            // Datasource prodided mappings
            return dataSource.getMappings();
        }       
        if( dataSource.isLocalizable() )
        {
            // Timestamp and location mappings
            return getTimestampAndLocationMappings( dataSource.getDataType( ) );
        }
        // Default timestamp mappings
        return  getTimestampMappings( dataSource.getDataType( ));
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
    
    /**
     * Build a JSON mappings block to declare 'timestamp' field as a date and 'location' field as a geo_point
     * 
     * @param strType
     *            The document type
     * @return The JSON
     */
    private static String getTimestampAndLocationMappings( String strType )
    {
        return "{ \"mappings\": { \"" + strType + "\" : { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"epoch_millis\" }, \"location\": { \"type\": \"geo_point\"} } }}}";
    }
    
}
