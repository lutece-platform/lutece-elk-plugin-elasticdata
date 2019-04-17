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

import org.apache.commons.lang.StringUtils;

import fr.paris.lutece.plugins.elasticdata.business.DataObject;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.elasticdata.business.IDataSourceExternalAttributesProvider;
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
                _mapDataSources = new HashMap<>( );
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
     * @param status
     *            the Indexing Status
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    public static void processFullIndexing( StringBuilder sbLogs, DataSource dataSource, boolean bReset, IndexingStatus status ) throws ElasticClientException
    {

        long timeBegin = System.currentTimeMillis( );
        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );
        if ( bReset )
        {
            if ( elastic.isExists( dataSource.getTargetIndexName( ) ) )
            {
                elastic.deleteIndex( dataSource.getTargetIndexName( ) );
            }
            elastic.createMappings( dataSource.getTargetIndexName( ), getMappings( dataSource ) );
        }

        int nBatchSize = ( dataSource.getBatchSize( ) != 0 ) ? dataSource.getBatchSize( ) : BATCH_SIZE;

        status.setnNbTotalObj( dataSource.getIdDataObjects( ).size( ) );

        // Index the objects in bulk mode
        int nbDocsInsert = insertObjects( elastic, dataSource, dataSource.getDataObjectsIterator( ), nBatchSize, status );

        long timeEnd = System.currentTimeMillis( );
        sbLogs.append( "Number of object inserted for Data Source '" ).append( dataSource.getName( ) ).append( "' : " ).append( nbDocsInsert );
        sbLogs.append( " (duration : " ).append( timeEnd - timeBegin ).append( "ms)\n" );

        clearData( dataSource );
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
    public static void processIncrementalIndexing( DataSource dataSource, DataObject dataObject ) throws ElasticClientException
    {
        completeDataObjectWithFullData( dataSource, dataObject );

        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );
        elastic.create( dataSource.getTargetIndexName( ), dataSource.getDataType( ), ( dataObject.getId( ) != null ) ? dataObject.getId( ) : StringUtils.EMPTY,
                dataObject );

    }

    /**
     * Insert a dataObject from a DataSource into Elastic Search
     * 
     * @param sbLogs
     *            A log buffer
     * @param dataSource
     *            The data source
     * @param dataObject
     *            The collection of data object
     * @throws fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException
     */
    public static void processIncrementalIndexing( StringBuilder sbLogs, DataSource dataSource, Collection<DataObject> dataObject )
            throws ElasticClientException
    {
        // Complete the data source with the external attributes
        provideExternalAttributes( dataSource );

        long timeBegin = System.currentTimeMillis( );
        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );

        int nBatchSize = ( dataSource.getBatchSize( ) != 0 ) ? dataSource.getBatchSize( ) : BATCH_SIZE;

        // Index the objects in bulk mode
        int nbDocsInsert = insertObjects( elastic, dataSource, dataObject.iterator( ), nBatchSize, new IndexingStatus( ) );

        long timeEnd = System.currentTimeMillis( );
        sbLogs.append( "Number of object inserted for Data Source '" ).append( dataSource.getName( ) ).append( "' : " ).append( nbDocsInsert );
        sbLogs.append( " (duration : " ).append( timeEnd - timeBegin ).append( "ms)\n" );

    }

    /**
     * Partial Updates to Documents
     * 
     * @param dataSource
     *            The data source
     * @param strId
     *            The document id
     * @param object
     *            The object
     * @throws ElasticClientException
     *             Exception If an error occurs accessing to ElasticSearch
     */
    public static void partialUpdate( DataSource dataSource, String strId, Object object ) throws ElasticClientException
    {

        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );
        elastic.partialUpdate( dataSource.getTargetIndexName( ), dataSource.getDataType( ), strId, object );

    }

    /**
     * Delete a documents by Query
     * 
     * @param dataSource
     *            The data source
     * @param strQuery
     *            The Query
     * @throws ElasticClientException
     *             Exception If an error occurs accessing to ElasticSearch
     */
    public static void deleteByQuery( DataSource dataSource, String strQuery ) throws ElasticClientException
    {

        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );
        elastic.deleteByQuery( dataSource.getTargetIndexName( ), dataSource.getDataType( ), strQuery );

    }

    /**
     * Delete a document based on its id in the index
     * 
     * @param dataSource
     *            The data source
     * @param strId
     *            The id
     * @throws ElasticClientException
     *             Exception If an error occurs accessing to ElasticSearch
     */
    public static void deleteById( DataSource dataSource, String strId ) throws ElasticClientException
    {

        String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
        Elastic elastic = new Elastic( strServerUrl );
        elastic.deleteDocument( dataSource.getTargetIndexName( ), dataSource.getDataType( ), strId );

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
        return insertDataAllDatasources( bReset, false );
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
    public static String insertDataAllDatasources( boolean bReset, boolean bDaemon ) throws ElasticClientException
    {
        StringBuilder sbLogs = new StringBuilder( );

        for ( DataSource dataSource : getDataSources( ) )
        {
            if ( dataSource.usesFullIndexingDaemon( ) || !bDaemon )
            {
                processFullIndexing( sbLogs, dataSource, bReset, new IndexingStatus( ) );
            }
        }
        return sbLogs.toString( );
    }

    /**
     * Return the mappings associated to a data source
     * 
     * @param dataSource
     *            The data source
     * @return The mappings string as JSON
     */
    private static String getMappings( DataSource dataSource )
    {
        if ( dataSource.getMappings( ) != null )
        {
            // Datasource prodided mappings
            return dataSource.getMappings( );
        }
        if ( dataSource.isLocalizable( ) )
        {
            // Timestamp and location mappings
            return getTimestampAndLocationMappings( dataSource.getDataType( ) );
        }
        // Default timestamp mappings
        return getTimestampMappings( dataSource.getDataType( ) );
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
        return "{ \"mappings\": { \""
                + strType
                + "\" : { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"epoch_millis\" }, \"location\": { \"type\": \"geo_point\"} } }}}";
    }

    /**
     * Clear the dataObjects from the data Source
     * 
     * @param dataSource
     *            the data source
     */
    public static void clearData( DataSource dataSource )
    {
        dataSource.removeDataObjects( );
    }

    public static void completeDataObjectWithFullData( DataSource dataSource, DataObject dataObject )
    {
        // Complete the data source with the external attributes
        provideExternalAttributes( dataSource, dataObject );
    }

    /**
     * Insert a list of object in bulk mode
     * 
     * @param elastic
     *            The Elastic Server
     * @param dataSource
     *            The data source
     * @param listDataObjects
     *            The list of objects
     * @param nBatchSize
     *            The number of objects in each bulk request
     * @throws ElasticClientException
     *             If a problem occurs connecting the server
     * @return the number of documents posted
     */
    private static int insertObjects( Elastic elastic, DataSource dataSource, Iterator<DataObject> iterateDataObjects, int nBatchSize, IndexingStatus status )
            throws ElasticClientException
    {
        List<DataObject> listBatch = new ArrayList<DataObject>( );

        int nCount = 0;

        while ( iterateDataObjects.hasNext( ) )
        {
            DataObject object = iterateDataObjects.next( );
            nCount++;
            status.setCurrentNbIndexedObj( nCount );
            listBatch.add( object );
            if ( ( listBatch.size( ) == nBatchSize ) || !iterateDataObjects.hasNext( ) )
            {
                BulkRequest br = new BulkRequest( );
                for ( DataObject batchObject : listBatch )
                {
                    String strId = batchObject.getId( );
                    IndexSubRequest isr = new IndexSubRequest( strId );
                    br.addAction( isr, batchObject );
                }
                AppLogService.info( "ElasticData indexing : Posting bulk action for " + listBatch.size( ) + " documents of DataSource '" + dataSource.getName( )
                        + "'" );
                if ( elastic == null )
                {
                    String strServerUrl = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
                    elastic = new Elastic( strServerUrl );
                }
                String strResponse = elastic.createByBulk( dataSource.getTargetIndexName( ), dataSource.getDataType( ), br );
                AppLogService.debug( "ElasticData : Response of the posted bulk request : " + strResponse );
                listBatch.clear( );
            }

        }
        AppLogService.info( "ElasticData indexing : completed for " + nCount + " documents of DataSource '" + dataSource.getName( ) + "'" );

        return nCount;
    }

    /**
     * Provide external attributes for the DataSource
     * 
     * @param dataSource
     *            the data source
     */
    private static void provideExternalAttributes( DataSource dataSource )
    {
        for ( IDataSourceExternalAttributesProvider provider : dataSource.getExternalAttributesProvider( ) )
        {
            provider.provideAttributes( dataSource );
        }
    }

    /**
     * Provide external attributes for the DataSource
     * 
     * @param dataSource
     *            the data source
     */
    private static void provideExternalAttributes( DataSource dataSource, DataObject dataObject )
    {
        for ( IDataSourceExternalAttributesProvider provider : dataSource.getExternalAttributesProvider( ) )
        {
            provider.provideAttributes( dataObject );
        }
    }
}
