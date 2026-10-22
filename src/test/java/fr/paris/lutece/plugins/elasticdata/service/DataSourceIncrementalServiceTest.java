/*
 * Copyright (c) 2002-2026, City of Paris
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
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import fr.paris.lutece.plugins.elasticdata.business.IndexerAction;
import fr.paris.lutece.plugins.elasticdata.business.IndexerActionHome;
import fr.paris.lutece.test.LuteceTestCase;

/**
 * Tests of the queue of the incremental indexing
 */
public class DataSourceIncrementalServiceTest extends LuteceTestCase
{
    private static final String DATA_SOURCE = "testIncrementalDataSource";
    private static final String RESOURCE = "42";
    private static final int THREADS = 8;
    private static final int ROUNDS = 20;

    /**
     * Removes the actions queued by the tests
     */
    @AfterEach
    protected void removeActions( )
    {
        for ( IndexerAction action : IndexerActionHome.getIndexerActionsList( DATA_SOURCE ) )
        {
            IndexerActionHome.remove( action.getId( ) );
        }
    }

    /**
     * A resource created and updated at the same time on several threads or instances (the CREATE and UPDATE events of one front office submission) is
     * queued once: the queue holds a single action per resource and data source
     *
     * @throws Exception
     *             if a thread fails
     */
    @Test
    public void testConcurrentAddTaskQueuesOneAction( ) throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( THREADS );
        try
        {
            for ( int nRound = 0; nRound < ROUNDS; nRound++ )
            {
                removeActions( );
                CyclicBarrier barrier = new CyclicBarrier( THREADS );
                List<Future<?>> listFutures = new ArrayList<>( );
                for ( int i = 0; i < THREADS; i++ )
                {
                    listFutures.add( executor.submit( ( ) -> {
                        barrier.await( 10, TimeUnit.SECONDS );
                        DataSourceIncrementalService.addTask( DATA_SOURCE, RESOURCE, IndexerAction.TASK_CREATE );
                        return null;
                    } ) );
                }
                for ( Future<?> future : listFutures )
                {
                    future.get( 30, TimeUnit.SECONDS );
                }
                assertEquals( 1, IndexerActionHome.getIndexerActionsList( DATA_SOURCE ).size( ), "round " + nRound );
            }
        }
        finally
        {
            executor.shutdownNow( );
        }
    }

    /**
     * Another instance queuing the same resource at the same time (out of reach of the lock of this instance) is stopped by the unique key of the table:
     * the second insertion is refused without error and the queue holds a single action
     */
    @Test
    public void testUniqueKeyRefusesSecondAction( )
    {
        assertTrue( IndexerActionHome.createIfAbsent( newAction( IndexerAction.TASK_CREATE ) ) );
        assertFalse( IndexerActionHome.createIfAbsent( newAction( IndexerAction.TASK_MODIFY ) ) );
        assertEquals( 1, IndexerActionHome.getIndexerActionsList( DATA_SOURCE ).size( ) );
    }

    /**
     * Builds an action of the test resource
     *
     * @param nIdTask
     *            the task id
     * @return the action
     */
    private static IndexerAction newAction( int nIdTask )
    {
        IndexerAction action = new IndexerAction( );
        action.setIdDataSource( DATA_SOURCE );
        action.setIdResource( RESOURCE );
        action.setIdTask( nIdTask );
        return action;
    }

    /**
     * A deletion queued after a creation of the same resource cancels it, and a deletion queued after a modification replaces it
     */
    @Test
    public void testDeleteAfterCreateOrModify( )
    {
        DataSourceIncrementalService.addTask( DATA_SOURCE, RESOURCE, IndexerAction.TASK_CREATE );
        DataSourceIncrementalService.addTask( DATA_SOURCE, RESOURCE, IndexerAction.TASK_DELETE );
        assertTrue( IndexerActionHome.getIndexerActionsList( DATA_SOURCE ).isEmpty( ) );

        DataSourceIncrementalService.addTask( DATA_SOURCE, RESOURCE, IndexerAction.TASK_MODIFY );
        DataSourceIncrementalService.addTask( DATA_SOURCE, RESOURCE, IndexerAction.TASK_DELETE );
        List<IndexerAction> listActions = IndexerActionHome.getIndexerActionsList( DATA_SOURCE );
        assertEquals( 1, listActions.size( ) );
        assertEquals( IndexerAction.TASK_DELETE, listActions.get( 0 ).getIdTask( ) );
    }
}
