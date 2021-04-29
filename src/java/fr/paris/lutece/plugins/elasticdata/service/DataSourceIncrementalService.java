package fr.paris.lutece.plugins.elasticdata.service;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;

import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.elasticdata.business.IndexerAction;
import fr.paris.lutece.plugins.elasticdata.business.IndexerActionHome;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.util.AppLogService;

public final class DataSourceIncrementalService
{

	private DataSourceIncrementalService() 
	{
		
	}
    /**
     * Insert Incremental data sources
     * 
     * @return The logs of the process
     * @throws ElasticClientException 
     * 
     */
    public static String processIncrementalIndexing( ) 
    {
        StringBuilder builder = new StringBuilder( );
        for ( DataSource dataSource : DataSourceService.getDataSources( ) )
        {
        	try {     		
				processIncrementalIndexing( dataSource );
        	} 
        	catch (ElasticClientException e) {
        		
                AppLogService.error( e.getMessage(), e );
				builder.append(e.getMessage( ));
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
     * 
     * @return The logs of the process
     * 
     */
    public static void processAsynchronouslyIncrementalIndexing( DataSource dataSource )
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
                        dataSource.getIndexingStatus( ).reset( );
                        processIncrementalIndexing( dataSource );
                       
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
         dataSource.getIndexingStatus( ).getSbLogs( ).append( " (duration : " ).append( System.currentTimeMillis( ) - timeBegin )
        .append( "ms)\n" );
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

    public static int processIncrementalIndexing( DataSource dataSource, List<String> listIdResource, int nIdTask ) throws ElasticClientException
    {

        Elastic elastic = DataSourceService.getElastic( );
        int nCount = 0;

        if ( elastic != null && !CollectionUtils.isEmpty( listIdResource ) )
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
               default://do nothing
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
