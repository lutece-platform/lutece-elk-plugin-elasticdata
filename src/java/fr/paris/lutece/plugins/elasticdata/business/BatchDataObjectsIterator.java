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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Implementation of Iterator<DataObject> for fetching object in DAO by batchs
 */
public class BatchDataObjectsIterator implements Iterator<DataObject>
{
    protected final int _nBatchSize;
    protected final List<String> _listIdDataObjects;
    protected final DataSource _dataSource;
    private Queue<DataObject> _queueTmpDataObject;
    private int _nNextFirstId = 0;

    public BatchDataObjectsIterator( DataSource dataSource, List<String> listIdDataObjects )
    {
        _queueTmpDataObject = new ConcurrentLinkedQueue<>( );
        _dataSource = dataSource;
        _nBatchSize = dataSource.getBatchSize( );
        _listIdDataObjects = listIdDataObjects;
        // Initialize the array of data objects with the firsts objects.
        List<String> listIdDataObjectsSublist = loadNextDataObjectsId( 0 );
        _nNextFirstId = _nBatchSize;
        _queueTmpDataObject.addAll( dataSource.getDataObjects( listIdDataObjectsSublist ) );

    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean hasNext( )
    {
        return ( _queueTmpDataObject.size( ) > 0 );
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public DataObject next( )
    {
        if ( _queueTmpDataObject.isEmpty( ) )
            throw new NoSuchElementException( );

        DataObject dataObj = _queueTmpDataObject.poll( );
        while ( _queueTmpDataObject.isEmpty( ) )
        {
            List<String> listIdDataObjectsSublist = loadNextDataObjectsId( _nNextFirstId );
            if ( !listIdDataObjectsSublist.isEmpty( ) )
            {
                _nNextFirstId += _nBatchSize;
                _queueTmpDataObject.addAll( _dataSource.getDataObjects( listIdDataObjectsSublist ) );
            }
            else
            {
                break;
            }
        }

        return dataObj;
    }

    /**
     * Load the next data objects ids
     * 
     * @return the next data objects ids
     */
    private List<String> loadNextDataObjectsId( int nFirstId )
    {
        // Initialize the array of data objects with the firsts objects.
        if ( _listIdDataObjects.size( ) < nFirstId + 1 )
        {
            return new ArrayList<>( );
        }
        else
        {
            int nLastId = nFirstId + _nBatchSize;
            if ( _listIdDataObjects.size( ) < nLastId )
            {
                nLastId = _listIdDataObjects.size( );
            }
            return _listIdDataObjects.subList( nFirstId, nLastId );
        }
    }
}
