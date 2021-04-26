package fr.paris.lutece.plugins.elasticdata.service;

import java.util.List;
import java.util.stream.Collectors;
import org.json.JSONArray;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.elasticdata.business.IndexerAction;
import fr.paris.lutece.plugins.elasticdata.business.IndexerActionHome;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.util.sql.TransactionManager;

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

        TransactionManager.beginTransaction( DataSourceUtils.getPlugin( ) );

        try
        {
            for ( DataSource dataSource : DataSourceService.getDataSources( ) )
            {
                {
                    int [ ] taskList = {
                            IndexerAction.TASK_CREATE, IndexerAction.TASK_MODIFY, IndexerAction.TASK_DELETE
                    };
                    for ( int nTask : taskList )
                    {
                        processIncrementalIndexing( dataSource, IndexerActionHome.getIndexerActionsList( dataSource.getId( ), nTask ), nTask );
                    }
                }
            }

        }
        catch( ElasticClientException e )
        {

            TransactionManager.rollBack( DataSourceUtils.getPlugin( ) );
            AppLogService.error( "Process intcremental indexing error: ", e );

        }

        TransactionManager.commitTransaction( DataSourceUtils.getPlugin( ) );

        return null;
    }

    /**
     * Process incremental indexing
     * 
     * @param dataSource
     *            the datasource
     * @param indexActionList
     *            the list of indexer actions to index
     * @param nIdTask
     *            the task id
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */

    public static void processIncrementalIndexing( DataSource dataSource, List<IndexerAction> indexerActionList, int nIdTask ) throws ElasticClientException
    {

        Elastic elastic = DataSourceService.getElastic( );
        List<String> idDataObjectList = indexerActionList.parallelStream( )
                .map( indexerAction -> IndexerAction.TASK_DELETE == nIdTask
                        ? DataSourceService.getIdDocument( dataSource.getId( ), indexerAction.getIdResource( ) )
                        : indexerAction.getIdResource( ) )
                .collect( Collectors.toList( ) );

        if ( elastic != null && idDataObjectList.size( ) > 0 )
        {
            switch( nIdTask )
            {
                case IndexerAction.TASK_CREATE:
                    DataSourceService.insertObjects( elastic, dataSource, dataSource.getDataObjectsIterator( idDataObjectList ) );
                    break;
                case IndexerAction.TASK_MODIFY:
                    DataSourceService.updateObjects( elastic, dataSource, dataSource.getDataObjectsIterator( idDataObjectList ) );
                    break;
                case IndexerAction.TASK_DELETE:
                    DataSourceService.deleteByQuery( dataSource, "{\"query\" : { \"terms\" : {\"_id\" : " + new JSONArray( idDataObjectList ) + "}}}" );
                    break;
            }

            for ( IndexerAction indexerAction : indexerActionList )
            {
                IndexerActionHome.remove( indexerAction.getId( ) );
            }
        }
    }

    /**
     * Create incremental task
     * 
     * @param strIdDataSource
     *            the datasource id
     * @param strIdResource
     *            the ressource id
     * @param nIdTask
     *            the task id
     * 
     */
    public static void addTask( String strIdDataSource, String strIdResource, int nIdTask )
    {

        IndexerAction indexerAction = IndexerActionHome.findByIdRessource( strIdResource, strIdDataSource );

        if ( indexerAction != null && nIdTask == IndexerAction.TASK_DELETE )
        {
            indexerAction.setIdTask( nIdTask );
            IndexerActionHome.update( indexerAction );
            return;
        }

        indexerAction = new IndexerAction( );
        indexerAction.setIdDataSource( strIdDataSource );
        indexerAction.setIdResource( strIdResource );
        indexerAction.setIdTask( nIdTask );
        IndexerActionHome.create( indexerAction );
    }

}
