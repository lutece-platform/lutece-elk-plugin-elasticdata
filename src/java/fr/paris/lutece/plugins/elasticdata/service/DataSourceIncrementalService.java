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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONArray;

import fr.paris.lutece.plugins.elasticdata.business.DataObject;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.elasticdata.business.IndexerAction;
import fr.paris.lutece.plugins.elasticdata.business.IndexerActionHome;
import fr.paris.lutece.plugins.libraryelastic.business.bulk.BulkRequest;
import fr.paris.lutece.plugins.libraryelastic.business.bulk.IndexSubRequest;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.util.sql.TransactionManager;
import jakarta.enterprise.concurrent.ManagedThreadFactory;
import jakarta.enterprise.inject.spi.CDI;

public final class DataSourceIncrementalService
{
	private static ManagedThreadFactory _threadFactory = CDI.current( ).select( ManagedThreadFactory.class ).get( );

    private DataSourceIncrementalService( )
    {

    }

    /**
     * Insert Incremental data sources
     * 
     * @return The logs of the process
     */
    public static String processIncrementalIndexing( )
    {
        StringBuilder builder = new StringBuilder( );
        for ( DataSource dataSource : DataSourceService.getDataSources( ) )
        {
            try
            {
                processIncrementalIndexing( dataSource );
            }
            catch( ElasticClientException e )
            {

                AppLogService.error( e.getMessage( ), e );
                builder.append( e.getMessage( ) );
            }
            builder.append( dataSource.getIndexingStatus( ).getSbLogs( ).toString( ) ).append( "\n" );

        }
        return builder.toString( );
    }

    /**
     * Insert Incremental data source
     * 
     * @param dataSource
     *            the data source
     */
    public static void processAsynchronouslyIncrementalIndexing( DataSource dataSource )
    {
        if ( dataSource.getIndexingStatus( ).getIsRunning( ).compareAndSet( false, true ) )
        {
        	Thread thread = _threadFactory.newThread( ( ) -> 
            {
                try
                {
                    processIncrementalIndexing( dataSource );

                }
                catch( ElasticClientException e )
                {
                    dataSource.getIndexingStatus( ).getSbLogs( ).append( e.getMessage( ) ).append( e );
                    AppLogService.error( "Process incremental indexing error: ", e );

                }
                finally
                {
                    dataSource.getIndexingStatus( ).getIsRunning( ).set( false );
                }
            } );
        	thread.start( );
        }

    }

    /**
     * process the incremental indexing of a data source
     * 
     * @param dataSource
     *            the data source
     * 
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     * 
     */
    public static void processIncrementalIndexing( DataSource dataSource ) throws ElasticClientException
    {
        dataSource.getIndexingStatus( ).reset( );
        int nCount = 0;
        int [ ] taskList = {
                IndexerAction.TASK_CREATE, IndexerAction.TASK_MODIFY, IndexerAction.TASK_DELETE
        };
        long timeBegin = System.currentTimeMillis( );
        for ( int nTask : taskList )
        {
            nCount += processIncrementalIndexing( dataSource, IndexerActionHome.getIdResourceIndexerActionsList( dataSource.getId( ), nTask ), nTask );

        }
        dataSource.getIndexingStatus( ).getSbLogs( ).append( "Number of documents processed by the incremental service from the Data Source '" )
                .append( dataSource.getName( ) ).append( "' : " ).append( nCount );
        dataSource.getIndexingStatus( ).getSbLogs( ).append( " (duration : " ).append( System.currentTimeMillis( ) - timeBegin ).append( "ms)\n" );
    }

    /**
     * Process incremental indexing of a data source according to the task
     * 
     * @param dataSource
     *            the datasource
     * @param listIdResource
     *            the list of resource id
     * @param nIdTask
     *            the task id
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     * @return the total count of documents processed
     */

    public static int processIncrementalIndexing( DataSource dataSource, List<String> listIdResource, int nIdTask ) throws ElasticClientException
    {

        Elastic elastic = DataSourceService.getElastic( );
        int nCount = 0;

        if ( elastic != null && listIdResource != null && !listIdResource.isEmpty( ) )
        {
            switch( nIdTask )
            {
                case IndexerAction.TASK_CREATE:
                    nCount += insertObjects( elastic, dataSource, dataSource.getDataObjectsIterator( listIdResource ) );
                    break;
                case IndexerAction.TASK_MODIFY:
                    nCount += updateObjects( elastic, dataSource, dataSource.getDataObjectsIterator( listIdResource ) );
                    break;
                case IndexerAction.TASK_DELETE:
                    nCount += deleteByQuery( dataSource, listIdResource );
                    break;
                default:// do nothing
            }
        }
        return nCount;
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
        List<String> listIdResource = new ArrayList<>( );
        while ( iterateDataObjects.hasNext( ) )
        {
            DataObject dataObject = iterateDataObjects.next( );
            listIdResource.add( dataObject.getId( ) );
            listBatch.add( dataObject );
            nCount++;
            if ( ( listBatch.size( ) == dataSource.getBatchSize( ) ) || !iterateDataObjects.hasNext( ) )
            {
                DataSourceService.completeDataObjectWithFullData( dataSource, listBatch );
                br = new BulkRequest( );
                for ( DataObject batchObject : listBatch )
                {
                    br.addAction( new IndexSubRequest( batchObject.getId( ) ), batchObject );
                }
                if ( elastic == null )
                {
                    elastic = DataSourceService.getElastic( );
                }

                try
                {
                    TransactionManager.beginTransaction( DataSourceUtils.getPlugin( ) );
                    String strResponse = elastic.createByBulk( dataSource.getTargetIndexName( ), br );
                    AppLogService.debug( "ElasticData : Response of the posted bulk request : {}", strResponse );

                    IndexerActionHome.removeByIdResourceList( listIdResource, dataSource.getId( ) );
                    listIdResource.clear( );
                    listBatch.clear( );

                    TransactionManager.commitTransaction( DataSourceUtils.getPlugin( ) );
                }
                catch( ElasticClientException e )
                {
                    TransactionManager.rollBack( DataSourceUtils.getPlugin( ) );
                    AppLogService.error( e.getMessage( ), e );
                    throw new ElasticClientException( "ElasticData createByBulk error", e );
                }
            }
            DataSourceService.updateIndexingStatus( dataSource, nCount );
        }
        AppLogService.debug( "ElasticData indexing : completed for {} documents of DataSource: {}", nCount, dataSource.getName( ) );

        return nCount;
    }

    /**
     * update a list of object
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
    public static int updateObjects( Elastic elastic, DataSource dataSource, Iterator<DataObject> iterateDataObjects ) throws ElasticClientException
    {
        List<DataObject> listBatch = new ArrayList<>( );
        List<String> listIdResource = new ArrayList<>( );
        int nCount = 0;
        if ( elastic == null )
        {
            elastic = DataSourceService.getElastic( );
        }

        while ( iterateDataObjects.hasNext( ) )
        {
            listBatch.add( iterateDataObjects.next( ) );
            if ( ( listBatch.size( ) == dataSource.getBatchSize( ) ) || !iterateDataObjects.hasNext( ) )
            {
                DataSourceService.completeDataObjectWithFullData( dataSource, listBatch );

                try
                {
                    TransactionManager.beginTransaction( DataSourceUtils.getPlugin( ) );
                    for ( DataObject batchObject : listBatch )
                    {
                        String strResponse = elastic.partialUpdate( dataSource.getTargetIndexName( ), batchObject.getId( ), batchObject );
                        AppLogService.debug( "ElasticData : Response of the partial update : {}", strResponse );
                        nCount++;
                    }
                    IndexerActionHome.removeByIdResourceList( listIdResource, dataSource.getId( ) );
                    listBatch.clear( );
                    TransactionManager.commitTransaction( DataSourceUtils.getPlugin( ) );
                }
                catch( ElasticClientException e )
                {
                    TransactionManager.rollBack( DataSourceUtils.getPlugin( ) );
                    AppLogService.error( e.getMessage( ), e );
                    throw new ElasticClientException( "ElasticData partialUpdate error", e );
                }
            }
            DataSourceService.updateIndexingStatus( dataSource, nCount );
        }
        AppLogService.info( "ElasticData partial update indexing : completed for {} documents of DataSource '{}'", nCount, dataSource.getName( ) );
        return nCount;
    }

    /**
     * Delete a documents by Query
     * 
     * @param dataSource
     *            The data source
     * @param listIdResource
     *            The list of resource identifiers
     * @throws ElasticClientException
     *             Exception If an error occurs accessing to ElasticSearch
     */
    public static int deleteByQuery( DataSource dataSource, List<String> listIdResource ) throws ElasticClientException
    {
        try
        {
            TransactionManager.beginTransaction( DataSourceUtils.getPlugin( ) );
            Elastic elastic = DataSourceService.getElastic( );
            List<String> idDocumentList = listIdResource.stream( ).map( idDataObject -> DataSourceService.getIdDocument( dataSource.getId( ), idDataObject ) )
                    .collect( Collectors.toList( ) );
            elastic.deleteByQuery( dataSource.getTargetIndexName( ), "{\"query\" : { \"terms\" : {\"_id\" : " + new JSONArray( idDocumentList ) + "}}}" );
            IndexerActionHome.removeByIdResourceList( listIdResource, dataSource.getId( ) );
            TransactionManager.commitTransaction( DataSourceUtils.getPlugin( ) );
            int nSize = idDocumentList.size( );
            DataSourceService.updateIndexingStatus( dataSource, nSize );
            return nSize;
        }
        catch( ElasticClientException e )
        {
            TransactionManager.rollBack( DataSourceUtils.getPlugin( ) );
            AppLogService.error( e.getMessage( ), e );
            throw new ElasticClientException( "ElasticData createByBulk error", e );
        }
    }

    /**
     * Create incremental task
     * 
     * @param strIdDataSource
     *            the datasource id
     * @param strIdResource
     *            the resource id
     * @param nIdTask
     *            the task id
     * 
     */
    public static void addTask( String strIdDataSource, String strIdResource, int nIdTask )
    {

        IndexerAction indexerAction = IndexerActionHome.findByIdResource( strIdResource, strIdDataSource );

        if ( indexerAction != null )
        {
            if ( indexerAction.getIdTask( ) == IndexerAction.TASK_CREATE && nIdTask == IndexerAction.TASK_DELETE )
            {
                IndexerActionHome.remove( indexerAction.getId( ) );
            }
            else
                if ( indexerAction.getIdTask( ) == IndexerAction.TASK_MODIFY && nIdTask == IndexerAction.TASK_DELETE )
                {
                    indexerAction.setIdTask( nIdTask );
                    IndexerActionHome.update( indexerAction );
                }
            return;
        }

        indexerAction = new IndexerAction( );
        indexerAction.setIdDataSource( strIdDataSource );
        indexerAction.setIdResource( strIdResource );
        indexerAction.setIdTask( nIdTask );
        IndexerActionHome.create( indexerAction );
    }

    /**
     * Load the data of all the datasource indexerAction objects and returns them as a list
     * 
     * @param strIdDataSource
     *            the identifier of data source
     * @return The list which contains the data of all the indexerAction objects
     */
    public static List<IndexerAction> getIndexerAction( String strIdDataSource ) {
        return IndexerActionHome.getIndexerActionsList( strIdDataSource );
    }

}
