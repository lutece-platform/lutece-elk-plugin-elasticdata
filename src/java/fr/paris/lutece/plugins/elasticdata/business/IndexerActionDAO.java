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

package fr.paris.lutece.plugins.elasticdata.business;

import fr.paris.lutece.portal.service.plugin.Plugin;
import fr.paris.lutece.util.ReferenceList;
import fr.paris.lutece.util.sql.DAOUtil;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class provides Data Access methods for IndexerAction objects
 */
public final class IndexerActionDAO implements IIndexerActionDAO
{
    // Constants
    private static final String SQL_QUERY_SELECT = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action WHERE id_action = ?";
    private static final String SQL_QUERY_SELECT_BY_ID_RESOURCE = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action WHERE id_resource = ? AND id_datasource = ?";
    private static final String SQL_QUERY_INSERT = "INSERT INTO elasticdata_indexer_action ( id_resource, id_task, id_datasource ) VALUES ( ?, ?, ? ) ";
    private static final String SQL_QUERY_DELETE = "DELETE FROM elasticdata_indexer_action WHERE id_action = ? ";
    private static final String SQL_QUERY_UPDATE = "UPDATE elasticdata_indexer_action SET id_action = ?, id_resource = ?, id_task = ?, id_datasource = ? WHERE id_action = ?";
    private static final String SQL_QUERY_SELECTALL = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action";
    private static final String SQL_QUERY_SELECTALL_ID = "SELECT id_action FROM elasticdata_indexer_action";
    private static final String SQL_QUERY_SELECTALL_ID_RESOURCE_BY_DATASOURCE_ID_TASK = "SELECT id_resource FROM elasticdata_indexer_action WHERE id_datasource = ? AND id_task = ?";
    private static final String SQL_QUERY_SELECTALL_BY_DATASOURCE_ID_TASK = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action WHERE id_datasource = ? AND id_task = ?";
    private static final String SQL_QUERY_SELECTALL_BY_DATASOURCE = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action WHERE id_datasource = ?";
    private static final String SQL_QUERY_SELECTALL_BY_IDS = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action WHERE id_action IN (  ";
    private static final String SQL_QUERY_DELETE_BY_LIST = "DELETE FROM elasticdata_indexer_action WHERE id_datasource = ? AND id_resource IN (?";
    private static final String SQL_CLOSE_PARENTHESIS = " ) ";
    private static final String SQL_ADITIONAL_PARAMETER = ",?";

    /**
     * {@inheritDoc }
     */
    @Override
    public void insert( IndexerAction indexerAction, Plugin plugin )
    {
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_INSERT, Statement.RETURN_GENERATED_KEYS, plugin ) )
        {
            int nIndex = 1;
            daoUtil.setString( nIndex++, indexerAction.getIdResource( ) );
            daoUtil.setInt( nIndex++, indexerAction.getIdTask( ) );
            daoUtil.setString( nIndex++, indexerAction.getIdDataSource( ) );

            daoUtil.executeUpdate( );
            if ( daoUtil.nextGeneratedKey( ) )
            {
                indexerAction.setId( daoUtil.getGeneratedKeyInt( 1 ) );
            }
        }

    }

    /**
     * {@inheritDoc }
     */
    @Override
    public Optional<IndexerAction> load( int nKey, Plugin plugin )
    {
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECT, plugin ) )
        {
	        daoUtil.setInt( 1 , nKey );
	        daoUtil.executeQuery( );
	        IndexerAction indexerAction = null;
	
	        if ( daoUtil.next( ) )
	        {
	            indexerAction = new IndexerAction();
	            int nIndex = 1;
	            
                indexerAction.setId( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
                indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );
	        }
	
	        return Optional.ofNullable( indexerAction );
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public IndexerAction loadByIdResource( String strIdResource, String strIdDataSource, Plugin plugin )
    {
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECT_BY_ID_RESOURCE, plugin ) )
        {
            daoUtil.setString( 1, strIdResource );
            daoUtil.setString( 2, strIdDataSource );
            daoUtil.executeQuery( );
            IndexerAction indexerAction = null;

            if ( daoUtil.next( ) )
            {
                indexerAction = new IndexerAction( );
                int nIndex = 1;

                indexerAction.setId( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
                indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );
            }

            return indexerAction;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void delete( int nKey, Plugin plugin )
    {
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_DELETE, plugin ) )
        {
            daoUtil.setInt( 1, nKey );
            daoUtil.executeUpdate( );
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void deleteByIdResourceList( List<String> listIdResource, String strIdDataSource, Plugin plugin )
    {
        int nlistIdResourceSize = listIdResource.size( );

        if ( nlistIdResourceSize > 0 )
        {
            StringBuilder sbSQL = new StringBuilder( SQL_QUERY_DELETE_BY_LIST );

            for ( int i = 1; i < nlistIdResourceSize; i++ )
            {
                sbSQL.append( SQL_ADITIONAL_PARAMETER );
            }

            sbSQL.append( SQL_CLOSE_PARENTHESIS );

            try ( DAOUtil daoUtil = new DAOUtil( sbSQL.toString( ), plugin ) )
            {
                daoUtil.setString( 1, strIdDataSource );

                for ( int i = 0; i < nlistIdResourceSize; i++ )
                {
                    daoUtil.setString( i + 2, listIdResource.get( i ) );
                }

                daoUtil.executeUpdate( );
            }
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void store( IndexerAction indexerAction, Plugin plugin )
    {
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_UPDATE, plugin ) )
        {
            int nIndex = 1;

            daoUtil.setInt( nIndex++, indexerAction.getId( ) );
            daoUtil.setString( nIndex++, indexerAction.getIdResource( ) );
            daoUtil.setInt( nIndex++, indexerAction.getIdTask( ) );
            daoUtil.setString( nIndex++, indexerAction.getIdDataSource( ) );
            daoUtil.setInt( nIndex, indexerAction.getId( ) );

            daoUtil.executeUpdate( );
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<IndexerAction> selectIndexerActionsList( Plugin plugin )
    {
        List<IndexerAction> indexerActionList = new ArrayList<>( );
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL, plugin ) )
        {
            daoUtil.executeQuery( );

            while ( daoUtil.next( ) )
            {
                IndexerAction indexerAction = new IndexerAction( );
                int nIndex = 1;

                indexerAction.setId( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
                indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );

                indexerActionList.add( indexerAction );
            }

            return indexerActionList;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<IndexerAction> selectIndexerActionsList( String strIdDataSource, int nIdTask, Plugin plugin )
    {

        List<IndexerAction> indexerActionList = new ArrayList<>( );
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL_BY_DATASOURCE_ID_TASK, plugin ) )
        {

            daoUtil.setString( 1, strIdDataSource );
            daoUtil.setInt( 2, nIdTask );

            daoUtil.executeQuery( );

            while ( daoUtil.next( ) )
            {
                IndexerAction indexerAction = new IndexerAction( );
                int nIndex = 1;

                indexerAction.setId( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
                indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );

                indexerActionList.add( indexerAction );
            }

            return indexerActionList;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<IndexerAction> selectIndexerActionsList( String strIdDataSource, Plugin plugin )
    {

        List<IndexerAction> indexerActionList = new ArrayList<>( );
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL_BY_DATASOURCE, plugin ) )
        {

            daoUtil.setString( 1, strIdDataSource );

            daoUtil.executeQuery( );

            while ( daoUtil.next( ) )
            {
                IndexerAction indexerAction = new IndexerAction( );
                int nIndex = 1;

                indexerAction.setId( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
                indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
                indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );

                indexerActionList.add( indexerAction );
            }

            return indexerActionList;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<String> selectIdResourceIndexerActionsList( String strIdDataSource, int nIdTask, Plugin plugin )
    {

        List<String> indexerActionList = new ArrayList<>( );
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL_ID_RESOURCE_BY_DATASOURCE_ID_TASK, plugin ) )
        {

            daoUtil.setString( 1, strIdDataSource );
            daoUtil.setInt( 2, nIdTask );

            daoUtil.executeQuery( );

            while ( daoUtil.next( ) )
            {
                indexerActionList.add( daoUtil.getString( 1 ) );

            }

            return indexerActionList;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<Integer> selectIdIndexerActionsList( Plugin plugin )
    {
        List<Integer> indexerActionList = new ArrayList<>( );
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL_ID, plugin ) )
        {
            daoUtil.executeQuery( );

            while ( daoUtil.next( ) )
            {
                indexerActionList.add( daoUtil.getInt( 1 ) );
            }

            return indexerActionList;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public ReferenceList selectIndexerActionsReferenceList( Plugin plugin )
    {
        ReferenceList indexerActionList = new ReferenceList( );
        try ( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL, plugin ) )
        {
            daoUtil.executeQuery( );

            while ( daoUtil.next( ) )
            {
                indexerActionList.addItem( daoUtil.getInt( 1 ), daoUtil.getString( 2 ) );
            }

            return indexerActionList;
        }
    }

        /**
     * {@inheritDoc }
     */
	@Override
	public List<IndexerAction> selectIndexerActionsListByIds( Plugin plugin, List<Integer> listIds ) {
		List<IndexerAction> indexerActionList = new ArrayList<>(  );
		
		StringBuilder builder = new StringBuilder( );

		if ( !listIds.isEmpty( ) )
		{
			for( int i = 0 ; i < listIds.size(); i++ ) {
			    builder.append( "?," );
			}
	
			String placeHolders =  builder.deleteCharAt( builder.length( ) -1 ).toString( );
			String stmt = SQL_QUERY_SELECTALL_BY_IDS + placeHolders + ")";
			
			
	        try ( DAOUtil daoUtil = new DAOUtil( stmt, plugin ) )
	        {
	        	int index = 1;
				for( Integer n : listIds ) {
					daoUtil.setInt(  index++, n ); 
				}
	        	
	        	daoUtil.executeQuery(  );
	        	while ( daoUtil.next(  ) )
		        {
                    IndexerAction indexerAction = new IndexerAction( );
                    int nIndex = 1;
    
                    indexerAction.setId( daoUtil.getInt( nIndex++ ) );
                    indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
                    indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
                    indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );
    
                    indexerActionList.add( indexerAction );
		        }
		
		        daoUtil.free( );
		        
	        }
	    }
		return indexerActionList;
		
	}
}
