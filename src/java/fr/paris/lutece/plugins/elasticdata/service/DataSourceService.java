/*
 * Copyright (c) 2002-2021, City of Paris
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
import org.apache.commons.lang3.StringUtils;
import fr.paris.lutece.plugins.elasticdata.business.DataObject;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.elasticdata.business.IDataSourceExternalAttributesProvider;
import fr.paris.lutece.plugins.libraryelastic.business.bulk.BulkRequest;
import fr.paris.lutece.plugins.libraryelastic.business.bulk.IndexSubRequest;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.business.event.ResourceEvent;
import fr.paris.lutece.portal.service.event.ResourceEventManager;
import fr.paris.lutece.portal.service.spring.SpringContextService;
import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.portal.service.util.AppPropertiesService;

/**
 * DataSourceService
 */
public final class DataSourceService
{
    private static final String PROPERTY_ELASTIC_SERVER_LOGIN = "elasticdata.elastic_server.login";
    private static final String PROPERTY_ELASTIC_SERVER_PWD = "elasticdata.elastic_server.pwd";
    private static final String PROPERTY_ELASTIC_SERVER_URL = "elasticdata.elastic_server.url";

    private static final String DEFAULT_ELASTIC_SERVER_URL = "httt://localhost:9200";
    private static final String SERVER_URL = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_URL, DEFAULT_ELASTIC_SERVER_URL );
    private static final String SERVER_LOGIN = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_LOGIN );
    private static final String SERVEUR_PWD = AppPropertiesService.getProperty( PROPERTY_ELASTIC_SERVER_PWD );

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
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    public static void processFullIndexing( DataSource dataSource, boolean bReset )
    {

        if ( dataSource.getIndexingStatus( ).getIsRunning( ).compareAndSet( false, true ) )
        {
            ( new Thread( )
            {
                @Override
                public void run( )
                {
                    process( dataSource, bReset );
                }
            } ).start( );
        }
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
    private static void process( DataSource dataSource, boolean bReset )
    {
        long timeBegin = System.currentTimeMillis( );
        dataSource.getIndexingStatus( ).reset( );
        try
        {
            Elastic elastic = getElastic( );
            if ( bReset )
            {
                if ( elastic.isExists( dataSource.getTargetIndexName( ) ) )
                {
                    elastic.deleteIndex( dataSource.getTargetIndexName( ) );
                }
                elastic.createMappings( dataSource.getTargetIndexName( ), getMappings( dataSource ) );
            }
            // Index the objects in bulk mode
            int nbDocsInsert = DataSourceIncrementalService.insertObjects( elastic, dataSource, dataSource.getDataObjectsIterator( ) );
            long timeEnd = System.currentTimeMillis( );
            dataSource.getIndexingStatus( ).getSbLogs( ).append( "Number of object inserted for Data Source '" ).append( dataSource.getName( ) )
                    .append( "' : " ).append( nbDocsInsert );
            dataSource.getIndexingStatus( ).getSbLogs( ).append( " (duration : " ).append( timeEnd - timeBegin ).append( "ms)\n" );

            ResourceEvent dataSourceFullIndexed = new ResourceEvent( );
            dataSourceFullIndexed.setIdResource( dataSource.getId( ) );
            dataSourceFullIndexed.setTypeResource( DataSourceUtils.RESOURCE_TYPE_INDEXING );
            ResourceEventManager.fireAddedResource( dataSourceFullIndexed );

        }
        catch( ElasticClientException e )
        {

            dataSource.getIndexingStatus( ).getSbLogs( ).append( e.getMessage( ) ).append( e );
            AppLogService.error( "Process full indexing: ", e );

        }
        finally
        {

            dataSource.getIndexingStatus( ).getIsRunning( ).set( false );
        }

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

        Elastic elastic = getElastic( );
        elastic.create( dataSource.getTargetIndexName( ), ( dataObject.getId( ) != null ) ? dataObject.getId( ) : StringUtils.EMPTY, dataObject );

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
        long timeBegin = System.currentTimeMillis( );
        Elastic elastic = getElastic( );
        // Index the objects in bulk mode
        int nbDocsInsert = insertObjects( elastic, dataSource, dataObject.iterator( ) );
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
        Elastic elastic = getElastic( );
        elastic.partialUpdate( dataSource.getTargetIndexName( ), getIdDocument( dataSource.getId( ), strId ), object );
    }

   
    /**
     * Delete a documents by Query
     * @param dataSource the data source
     * @param strQuery the query
     * @throws ElasticClientException Exception If an error occurs accessing to ElasticSearch
     */
    public static void deleteByQuery( DataSource dataSource, String strQuery ) throws ElasticClientException
    {
         Elastic elastic = getElastic( );
         elastic.deleteByQuery( dataSource.getTargetIndexName( ), strQuery );
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
        Elastic elastic = getElastic( );
        elastic.deleteDocument( dataSource.getTargetIndexName( ), getIdDocument( dataSource.getId( ), strId ) );
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
    public static String insertDataAllDatasources( boolean bReset )
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
     */
    public static String insertDataAllDatasources( boolean bReset, boolean bDaemon )
    {
        StringBuilder builder = new StringBuilder( );
        for ( DataSource dataSource : getDataSources( ) )
        {
            if ( ( dataSource.usesFullIndexingDaemon( ) || !bDaemon ) )
            {
                process( dataSource, bReset );
                builder.append( dataSource.getIndexingStatus( ).getSbLogs( ).toString( ) ).append( "\n" );
            }
        }
        return builder.toString( );
    }

    /**
     * Return elastic connection
     **/
    public static Elastic getElastic( )
    {
        Elastic elastic = null;
        if ( StringUtils.isNotEmpty( SERVER_LOGIN ) && StringUtils.isNotEmpty( SERVEUR_PWD ) )
        {
            elastic = new Elastic( SERVER_URL, SERVER_LOGIN, SERVEUR_PWD );
        }
        else
        {
            elastic = new Elastic( SERVER_URL );
        }
        return elastic;
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
            return DataSourceUtils.TIMESTAMP_AND_LOCATION_MAPPINGS;
        }
        // Default timestamp mappings
        return DataSourceUtils.TIMESTAMP_MAPPINGS;
    }

    /**
     * Complete the data source with the external attributes and set elastic docuement id
     * 
     * @param dataSource
     *            the data source
     * @param dataObject
     *            the data object
     */
    public static void completeDataObjectWithFullData( DataSource dataSource, DataObject dataObject )
    {
        dataObject.setId( getIdDocument( dataSource.getId( ), dataObject.getId( ) ) );
        provideExternalAttributes( dataSource, dataObject );
    }

    /**
     * Complete the data source with the external attributes and set elastic docuement id
     * 
     * @param dataSource
     *            the data source
     * @param dataObject
     *            the data object
     */
    public static void completeDataObjectWithFullData( DataSource dataSource, List<DataObject> dataObjectList )
    {
        for ( DataObject dataObject : dataObjectList )
        {
            dataObject.setId( getIdDocument( dataSource.getId( ), dataObject.getId( ) ) );
        }
        provideExternalAttributes( dataSource, dataObjectList );
    }

    /**
     * Return the unique id of the elastic document
     * 
     * @param strIdDataSource
     *            the data source id
     * @param strIdDataObject
     *            the data object id
     */
    public static String getIdDocument( String strIdDataSource, String strIdDataObject )
    {
        return DataSourceUtils.PREFIX_DATA_OBJECT_ID + strIdDataSource + "_" + strIdDataObject;
    }

    /**
     * Insert a list of object in bulk mode
     * 
     * @param elastic
     *            The Elastic Server
     * @param dataSource
     *            The data source
     * @param iterateDataObjects
     *            The iterator of objects
     * @throws ElasticClientException
     *             If a problem occurs connecting the server
     * @return the number of documents posted
     */
    public static int insertObjects( Elastic elastic, DataSource dataSource, Iterator<DataObject> iterateDataObjects ) throws ElasticClientException
    {
        List<DataObject> listBatch = new ArrayList<>( );
        int nCount = 0;
        BulkRequest br;
        while ( iterateDataObjects.hasNext( ) )
        {
            DataObject dataObject = iterateDataObjects.next( );
            listBatch.add( dataObject );
            nCount++;
            if ( ( listBatch.size( ) == dataSource.getBatchSize( ) ) || !iterateDataObjects.hasNext( ) )
            {
                completeDataObjectWithFullData( dataSource, listBatch );
                br = new BulkRequest( );
                for ( DataObject batchObject : listBatch )
                {
                    br.addAction( new IndexSubRequest( batchObject.getId( ) ), batchObject );
                }
                if ( elastic == null )
                {
                    elastic = getElastic( );
                }
                String strResponse = elastic.createByBulk( dataSource.getTargetIndexName( ), br );
                AppLogService.debug( "ElasticData : Response of the posted bulk request : " + strResponse );
                    listBatch.clear( );
            }
            updateIndexingStatus( dataSource, nCount );
        }
        AppLogService.debug( "ElasticData indexing : completed for " + nCount + " documents of DataSource: " + dataSource.getName( ) );
        
        return nCount;
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

    /**
     * Provide external attributes for the DataSource
     * 
     * @param dataSource
     *            the data source
     * @param listDataObject
     *            list of data objects
     */
    private static void provideExternalAttributes( DataSource dataSource, List<DataObject> listDataObject )
    {
        for ( IDataSourceExternalAttributesProvider provider : dataSource.getExternalAttributesProvider( ) )
        {
            provider.provideAttributes( listDataObject );
        }
    }

    /**
     * Update indexing status of the datasource
     * 
     * @param dataSource
     *            the data source
     * @param nCount
     *            the count of objects processed
     */
    public static void updateIndexingStatus( DataSource dataSource, int nCount )
    {
        if ( dataSource.getIndexingStatus( ).getNbTotalObj( ) < nCount )
        {
            dataSource.getIndexingStatus( ).setnNbTotalObj( nCount );
        }
        dataSource.getIndexingStatus( ).setCurrentNbIndexedObj( nCount );
    }

}
