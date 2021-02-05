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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class IndexingStatusService
{

    private Map<String, IndexingStatus> _mapIndexingStatus = new HashMap<>( );
    private static IndexingStatusService _instance = new IndexingStatusService( );
    ObjectMapper _mapper = new ObjectMapper( );

    /**
     * Private constructor
     */
    private IndexingStatusService( )
    {
        // Do nothing
    }

    /**
     * Get the instance of the indexing status service
     * 
     * @return The instance of the indexing status service
     */
    public static IndexingStatusService getInstance( )
    {
        return _instance;
    }

    /**
     * Get the indexing status
     * 
     * @param strDateSourceId
     *            the data source id
     * 
     * @return the indexing status
     */
    public IndexingStatus getIndexingStatus( String strDataSourceId )
    {
        return _mapIndexingStatus.get( strDataSourceId );
    }

    /**
     * Register the indexing status
     * 
     * @param strDateSourceId
     *            the data source id
     * @param status
     *            the indexing status
     * 
     */
    public void registerIndexingStatus( String strDataSourceId, IndexingStatus status )
    {
        _mapIndexingStatus.put( strDataSourceId, status );
    }

    /**
     * Remove the indexing status
     * 
     * @param strDateSourceId
     *            the data source id
     * @param status
     *            the indexing status
     */
    public void removeIndexingStatus( String strDataSourceId, IndexingStatus status )
    {
        _mapIndexingStatus.remove( strDataSourceId );
    }

    /**
     * Get the total number of data objects to index
     * 
     * @param strDateSourceId
     *            the data source id
     * 
     * @return the total number or data objects to index
     */
    public int getNbTotalObj( String strDataSourceId )
    {
        IndexingStatus status = _mapIndexingStatus.get( strDataSourceId );
        if ( status != null )
        {
            return status.getNbTotalObj( );
        }
        return 0;
    }

    /**
     * Set the total number of data objects to index
     * 
     * @param nNbTotalObj
     *            The total number of objs
     * @param strDateSourceId
     *            the data source id
     */
    public void setnNbTotalObj( int nNbTotalObj, String strDataSourceId )
    {
        IndexingStatus status = _mapIndexingStatus.get( strDataSourceId );
        if ( status != null )
        {
            status.setnNbTotalObj( nNbTotalObj );
        }
    }

    /**
     * Get the current number of indexed objects
     * 
     * @param strDateSourceId
     *            the data source id
     * @return the current number of indexed objects
     */
    public int getCurrentNbIndexedObj( String strDataSourceId )
    {
        IndexingStatus status = _mapIndexingStatus.get( strDataSourceId );
        if ( status != null )
        {
            return status.getCurrentNbIndexedObj( );
        }
        return 0;
    }

    /**
     * Set the current number of indexed objects
     * 
     * @param nCurrentNbIndexedObj
     *            the current
     * @param strDateSourceId
     *            the data source id
     * 
     */
    public void setCurrentNbIndexedObj( int nCurrentNbIndexedObj, String strDataSourceId )
    {
        IndexingStatus status = _mapIndexingStatus.get( strDataSourceId );
        if ( status != null )
        {
            status.setCurrentNbIndexedObj( nCurrentNbIndexedObj );
        }
    }

    /**
     * Get the percent of indexed objects
     * 
     * @param strDateSourceId
     *            the data source id
     * @return the percent of the indexed objects
     */
    public double getPercent( String strDataSourceId )
    {
        IndexingStatus status = _mapIndexingStatus.get( strDataSourceId );
        if ( status != null )
        {
            double dPercent = (double) status.getCurrentNbIndexedObj( ) / (double) status.getNbTotalObj( ) * 100.0;
            return dPercent;
        }
        return 0;
    }
}
