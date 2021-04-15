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

/**
 * This class provides Data Access methods for IndexerAction objects
 */
public final class IndexerActionDAO implements IIndexerActionDAO
{
    // Constants
    private static final String SQL_QUERY_SELECT = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action WHERE id_action = ?";
    private static final String SQL_QUERY_INSERT = "INSERT INTO elasticdata_indexer_action ( id_resource, id_task, id_datasource ) VALUES ( ?, ?, ? ) ";
    private static final String SQL_QUERY_DELETE = "DELETE FROM elasticdata_indexer_action WHERE id_action = ? ";
    private static final String SQL_QUERY_UPDATE = "UPDATE elasticdata_indexer_action SET id_action = ?, id_resource = ?, id_task = ?, id_datasource = ? WHERE id_action = ?";
    private static final String SQL_QUERY_SELECTALL = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action";
    private static final String SQL_QUERY_SELECTALL_ID = "SELECT id_action FROM elasticdata_indexer_action";
    private static final String SQL_QUERY_SELECTALL_ID_RESSOURCE_BY_DATASOURCE_ID_TASK = "SELECT id_resource FROM elasticdata_indexer_action WHERE id_datasource = ? AND id_task = ?";
    private static final String SQL_QUERY_SELECTALL_BY_DATASOURCE_ID_TASK = "SELECT id_action, id_resource, id_task, id_datasource FROM elasticdata_indexer_action WHERE id_datasource = ? AND id_task = ?";


    /**
     * {@inheritDoc }
     */
    @Override
    public void insert( IndexerAction indexerAction, Plugin plugin )
    {
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_INSERT, Statement.RETURN_GENERATED_KEYS, plugin ) )
        {
            int nIndex = 1;
            daoUtil.setString( nIndex++ , indexerAction.getIdResource( ) );
            daoUtil.setInt( nIndex++ , indexerAction.getIdTask( ) );
            daoUtil.setString( nIndex++ , indexerAction.getIdDataSource( ) );
            
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
    public IndexerAction load( int nKey, Plugin plugin )
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
	
	        daoUtil.free( );
	        return indexerAction;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void delete( int nKey, Plugin plugin )
    {
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_DELETE, plugin ) )
        {
	        daoUtil.setInt( 1 , nKey );
	        daoUtil.executeUpdate( );
	        daoUtil.free( );
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void store( IndexerAction indexerAction, Plugin plugin )
    {
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_UPDATE, plugin ) )
        {
	        int nIndex = 1;
	        
	        daoUtil.setInt( nIndex++ , indexerAction.getId( ) );
	        daoUtil.setString( nIndex++ , indexerAction.getIdResource( ) );
	        daoUtil.setInt( nIndex++ , indexerAction.getIdTask( ) );
	        daoUtil.setString( nIndex++ , indexerAction.getIdDataSource( ) );
	        daoUtil.setInt( nIndex , indexerAction.getId( ) );
	
	        daoUtil.executeUpdate( );
	        daoUtil.free( );
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<IndexerAction> selectIndexerActionsList( Plugin plugin )
    {
        List<IndexerAction> indexerActionList = new ArrayList<>(  );
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL, plugin ) )
        {
	        daoUtil.executeQuery(  );
	
	        while ( daoUtil.next(  ) )
	        {
	            IndexerAction indexerAction = new IndexerAction(  );
	            int nIndex = 1;
	            
	            indexerAction.setId( daoUtil.getInt( nIndex++ ) );
	            indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
	            indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
	            indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );            
	
	            indexerActionList.add( indexerAction );
	        }
	
	        daoUtil.free( );
	        return indexerActionList;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<IndexerAction> selectIndexerActionsList( String strIdDataSource, int nIdTask, Plugin plugin )
    {
        
        List<IndexerAction> indexerActionList = new ArrayList<>(  );
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL_BY_DATASOURCE_ID_TASK, plugin ) )
        {

            daoUtil.setString( 1 , strIdDataSource );
            daoUtil.setInt( 2 , nIdTask );

	        daoUtil.executeQuery(  );
	
	        while ( daoUtil.next(  ) )
	        {
	            IndexerAction indexerAction = new IndexerAction(  );
	            int nIndex = 1;
	            
	            indexerAction.setId( daoUtil.getInt( nIndex++ ) );
	            indexerAction.setIdResource( daoUtil.getString( nIndex++ ) );
	            indexerAction.setIdTask( daoUtil.getInt( nIndex++ ) );
	            indexerAction.setIdDataSource( daoUtil.getString( nIndex ) );            
	
	            indexerActionList.add( indexerAction );
	        }
	
	        daoUtil.free( );
	        return indexerActionList;
        }
    }

        /**
     * {@inheritDoc }
     */
    @Override
    public List<String> selectIdRessourceIndexerActionsList( String strIdDataSource, int nIdTask, Plugin plugin )
    {
        
        List<String> indexerActionList = new ArrayList<>(  );
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL_ID_RESSOURCE_BY_DATASOURCE_ID_TASK, plugin ) )
        {

            daoUtil.setString( 1 , strIdDataSource );
            daoUtil.setInt( 2 , nIdTask );

	        daoUtil.executeQuery(  );
	
	        while ( daoUtil.next(  ) )
	        {
	            indexerActionList.add( daoUtil.getString( 1 ) );

	        }
	
	        daoUtil.free( );
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
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL_ID, plugin ) )
        {
	        daoUtil.executeQuery(  );
	
	        while ( daoUtil.next(  ) )
	        {
	            indexerActionList.add( daoUtil.getInt( 1 ) );
	        }
	
	        daoUtil.free( );
	        return indexerActionList;
        }
    }
    
    /**
     * {@inheritDoc }
     */
    @Override
    public ReferenceList selectIndexerActionsReferenceList( Plugin plugin )
    {
        ReferenceList indexerActionList = new ReferenceList();
        try( DAOUtil daoUtil = new DAOUtil( SQL_QUERY_SELECTALL, plugin ) )
        {
	        daoUtil.executeQuery(  );
	
	        while ( daoUtil.next(  ) )
	        {
	            indexerActionList.addItem( daoUtil.getInt( 1 ) , daoUtil.getString( 2 ) );
	        }
	
	        daoUtil.free( );
	        return indexerActionList;
    	}
    }
}
