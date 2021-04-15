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
public class DataSourceIncrementalService {


    /**
     * Insert Incremental data sources
     * 
     * @return The logs of the process
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */
    public static String insertDataIncrementalDatasources() {
        try {
            for (DataSource dataSource: DataSourceService.getDataSources()) {
                {
                    String strIdDataSource = dataSource.getId();
                    int[] taskList = {
                        IndexerAction.TASK_CREATE,
                        IndexerAction.TASK_MODIFY,
                        IndexerAction.TASK_DELETE
                    };
                    for (int nTask: taskList) {
                        processIncrementalIndexing(dataSource, IndexerActionHome.getIndexerActionsList(strIdDataSource, nTask), nTask);
                    }
                }
            }

        } catch (ElasticClientException e) {

            AppLogService.error("Process intcremental indexing error: ", e);

        }
        return null;
    }

    /**
     * Process incremental indexing
     * 
     * @param dataSource the datasource
     * @param indexActionList the list of indexer actions to index
     * @param nTask the task id
     * @throws ElasticClientException
     *             If an error occurs accessing to ElasticSearch
     */

    public static void processIncrementalIndexing(DataSource dataSource, List < IndexerAction > indexerActionList, int nTask) throws ElasticClientException {

        List < String > idDataObjectList = indexerActionList.parallelStream( ).map( indexerAction -> indexerAction.getIdResource( ) ).collect( Collectors.toList( ) );
        Elastic elastic = DataSourceService.getElastic( );

        switch (nTask) {
            case IndexerAction.TASK_CREATE:
                DataSourceService.insertObjects( elastic, dataSource, dataSource.getIncrementalDataObjectsIterator( idDataObjectList ) );
                break;
            case IndexerAction.TASK_MODIFY:
                DataSourceService.updateObjects( elastic, dataSource, dataSource.getIncrementalDataObjectsIterator( idDataObjectList ) );
                break;
            case IndexerAction.TASK_DELETE:
                DataSourceService.deleteByQuery( dataSource, "{\"query\" : { \"terms\" : {\"_id\" : " + new JSONArray(idDataObjectList) + "}}}" );
                break;
        }

        for ( IndexerAction indexerAction: indexerActionList ) {
            IndexerActionHome.remove( indexerAction.getId( ) );
        }
    }

    /**
     * Create incremental task
     * 
     * @param strIdDataSource the datasource id
     * @param strIdResource the ressource id
     * @param nTask the task id
     * 
     */

    public static void addTask( String strIdDataSource, String strIdResource, int nIdTask ) {

    IndexerAction indexerAction = new IndexerAction( );
    indexerAction.setIdDataSource( strIdDataSource );
    indexerAction.setIdResource( strIdResource );
    indexerAction.setIdTask( nIdTask );
    IndexerActionHome.create( indexerAction );

    }

}