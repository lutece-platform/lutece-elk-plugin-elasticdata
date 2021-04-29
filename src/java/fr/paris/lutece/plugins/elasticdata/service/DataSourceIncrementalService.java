package fr.paris.lutece.plugins.elasticdata.service;

import java.util.List;
import java.util.stream.Collectors;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.elasticdata.business.IndexerAction;
import fr.paris.lutece.plugins.elasticdata.business.IndexerActionHome;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.util.AppLogService;

public final class DataSourceIncrementalService
{

    /**
     * Insert Incremental data sources
     * 
     * @return The logs of the process
     * 
     */
    public static String insertDataIncrementalDatasources( )
    {
        for ( DataSource dataSource : DataSourceService.getDataSources( ) )
        {
            insertDataIncrementalDatasource( dataSource );
        }

        return null;
    }

    /**
     * Insert Incremental data source
     * 
     * @param dataSource
     *            the data source
     * 
     * @return The logs of the process
     * 
     */
    public static String insertDataIncrementalDatasource( DataSource dataSource )
    {
        if ( dataSource.getIndexingStatus( ).getIsRunning( ).compareAndSet( false, true ) )
        {
            ( new Thread( )
            {
                @Override
                public void run( )
                {
                    try
                    {
                        long timeBegin = System.currentTimeMillis( );
                        dataSource.getIndexingStatus( ).reset( );

                        int nCount = processIncrementalIndexing( dataSource );
                        dataSource.getIndexingStatus( ).getSbLogs( ).append( "Number of documents processed by the incremental service from the Data Source '" )
                                .append( dataSource.getName( ) ).append( "' : " ).append( nCount );
                        dataSource.getIndexingStatus( ).getSbLogs( ).append( " (duration : " ).append( System.currentTimeMillis( ) - timeBegin )
                                .append( "ms)\n" );

                    }
                    catch( ElasticClientException e )
                    {
                        dataSource.getIndexingStatus( ).getSbLogs( ).append( e.getMessage( ) ).append( e );
                        AppLogService.error( "Process intcremental indexing error: ", e );

                    }
                    finally
                    {
                        dataSource.getIndexingStatus( ).getIsRunning( ).set( false );
                    }

                }
            } ).start( );
        }

        return null;
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
     * @return The total count of documents processed
     * 
     */
    public static int processIncrementalIndexing( DataSource dataSource ) throws ElasticClientException
    {
        int nCount = 0;
        int [ ] taskList = {
                IndexerAction.TASK_CREATE, IndexerAction.TASK_MODIFY, IndexerAction.TASK_DELETE
        };
        for ( int nTask : taskList )
        {
            nCount += processIncrementalIndexing( dataSource, IndexerActionHome.getIndexerActionsList( dataSource.getId( ), nTask ), nTask );
        }
        return nCount;
    }

    /**
     * Process incremental indexing of a data source according to the task
     * 
     * @param dataSource
     *            the datasource
     * @param indexActionList
     *            the list of indexer actions to index
     * @param nIdTask
     *            the task id
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     * @return the total count of documents processed
     */

    public static int processIncrementalIndexing( DataSource dataSource, List<IndexerAction> indexerActionList, int nIdTask ) throws ElasticClientException
    {

        Elastic elastic = DataSourceService.getElastic( );
        List<String> listIdResource = indexerActionList.parallelStream( ).map( indexerAction -> indexerAction.getIdResource( ) )
                .collect( Collectors.toList( ) );
        int nCount = 0;

        if ( elastic != null && listIdResource.size( ) > 0 )
        {
            switch( nIdTask )
            {
                case IndexerAction.TASK_CREATE:
                    nCount += DataSourceService.insertObjects( elastic, dataSource, dataSource.getDataObjectsIterator( listIdResource ) );
                    break;
                case IndexerAction.TASK_MODIFY:
                    nCount += DataSourceService.updateObjects( elastic, dataSource, dataSource.getDataObjectsIterator( listIdResource ) );
                    break;
                case IndexerAction.TASK_DELETE:
                    nCount += DataSourceService.deleteByQuery( dataSource, listIdResource );
                    break;
            }
        }
        return nCount;
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

}
