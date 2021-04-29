package fr.paris.lutece.plugins.elasticdata.service.daemon;

import fr.paris.lutece.plugins.elasticdata.service.DataSourceIncrementalService;
import fr.paris.lutece.portal.service.daemon.Daemon;

public class IncrementalIndexingDaemon extends Daemon
{
    /**
     * {@inheritDoc }
     */
    @Override
    public void run( )
    {
        setLastRunLogs( DataSourceIncrementalService.insertDataIncrementalDatasources( ) );
    }
}
