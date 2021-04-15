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
import fr.paris.lutece.portal.service.plugin.PluginService;
import fr.paris.lutece.portal.service.spring.SpringContextService;
import fr.paris.lutece.util.ReferenceList;

import java.util.List;

/**
 * This class provides instances management methods (create, find, ...) for IndexerAction objects
 */
public final class IndexerActionHome
{
    // Static variable pointed at the DAO instance
    private static IIndexerActionDAO _dao = SpringContextService.getBean( "elasticdata.indexerActionDAO" );
    private static Plugin _plugin = PluginService.getPlugin( "elasticdata" );

    /**
     * Private constructor - this class need not be instantiated
     */
    private IndexerActionHome(  )
    {
    }

    /**
     * Create an instance of the indexerAction class
     * @param indexerAction The instance of the IndexerAction which contains the informations to store
     * @return The  instance of indexerAction which has been created with its primary key.
     */
    public static IndexerAction create( IndexerAction indexerAction )
    {
        _dao.insert( indexerAction, _plugin );

        return indexerAction;
    }

    /**
     * Update of the indexerAction which is specified in parameter
     * @param indexerAction The instance of the IndexerAction which contains the data to store
     * @return The instance of the  indexerAction which has been updated
     */
    public static IndexerAction update( IndexerAction indexerAction )
    {
        _dao.store( indexerAction, _plugin );

        return indexerAction;
    }

    /**
     * Remove the indexerAction whose identifier is specified in parameter
     * @param nKey The indexerAction Id
     */
    public static void remove( int nKey )
    {
        _dao.delete( nKey, _plugin );
    }

    /**
     * Returns an instance of a indexerAction whose identifier is specified in parameter
     * @param nKey The indexerAction primary key
     * @return an instance of IndexerAction
     */
    public static IndexerAction findByPrimaryKey( int nKey )
    {
        return _dao.load( nKey, _plugin );
    }

    /**
     * Load the data of all the indexerAction objects and returns them as a list
     * @return the list which contains the data of all the indexerAction objects
     */
    public static List<IndexerAction> getIndexerActionsList( )
    {
        return _dao.selectIndexerActionsList( _plugin );
    }

    /**
     * Load the data of all the indexerAction objects and returns them as a list
     * @return the list which contains the data of all the indexerAction objects
     */
    public static List<IndexerAction> getIndexerActionsList( String strIdDataSource, int nIdTask )
    {
        return _dao.selectIndexerActionsList( strIdDataSource, nIdTask, _plugin );
    }
    
    /**
     * Load the id of all the indexerAction objects and returns them as a list
     * @return the list which contains the id of all the indexerAction objects
     */
    public static List<Integer> getIdIndexerActionsList( )
    {
        return _dao.selectIdIndexerActionsList( _plugin );
    }

     /**
     * Load the id of all the indexerAction objects and returns them as a list
     * @return the list which contains the id of all the indexerAction objects
     */
    public static List<String> getIdRessourceIndexerActionsList( String strIdDataSource, int nIdTask  )
    {
        return _dao.selectIdRessourceIndexerActionsList( strIdDataSource, nIdTask, _plugin );
    }
    
    
    /**
     * Load the data of all the indexerAction objects and returns them as a referenceList
     * @return the referenceList which contains the data of all the indexerAction objects
     */
    public static ReferenceList getIndexerActionsReferenceList( )
    {
        return _dao.selectIndexerActionsReferenceList( _plugin );
    }
}

