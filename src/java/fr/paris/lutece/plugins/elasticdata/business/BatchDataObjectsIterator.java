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

import fr.paris.lutece.portal.service.util.AppPropertiesService;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * Implementation of Iterator<DataObject> for fetching object in DAO by batchs
 */
public class BatchDataObjectsIterator implements Iterator<DataObject>
{
    private static final String PROPERTY_BULK_BATCH_SIZE = "elasticdata.bulk_batch_size";
    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final int BATCH_SIZE = AppPropertiesService.getPropertyInt( PROPERTY_BULK_BATCH_SIZE, DEFAULT_BATCH_SIZE );
    protected final int _nBatchSize;
    protected final List<String> _listIdDataObjects;
    protected final DataSource _dataSource;
    private LinkedHashMap<String, DataObject> _mapTmpIdDataObject;
    private int _nNextFirstId = 0;

    public BatchDataObjectsIterator( DataSource dataSource )
    {
        _mapTmpIdDataObject = new LinkedHashMap<>( );
        _dataSource = dataSource;
        _nBatchSize = ( dataSource.getBatchSize( ) < 1 ) ? BATCH_SIZE : dataSource.getBatchSize( );
        _listIdDataObjects = dataSource.getIdDataObjects( );

        // Initialize the array of data objects with the firsts objects.
        List<String> listIdDataObjectsSublist = loadNextDataObjectsId( 0 );
        _nNextFirstId = _nBatchSize;

        for ( DataObject obj : dataSource.getDataObjects( listIdDataObjectsSublist ) )
        {
            _mapTmpIdDataObject.put( obj.getId( ), obj );
        }

    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean hasNext( )
    {
        return ( _mapTmpIdDataObject.size( ) > 0 );
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public DataObject next( )
    {
        if ( _mapTmpIdDataObject.isEmpty( ) )
            return null;

        Optional<DataObject> optDataObject = _mapTmpIdDataObject.values( ).stream( ).findFirst( );
        DataObject dataObj = optDataObject.get( );
        if ( dataObj != null )
        {
            _mapTmpIdDataObject.remove( dataObj.getId( ) );

            if ( _mapTmpIdDataObject.isEmpty( ) )
            {
                List<String> listIdDataObjectsSublist = loadNextDataObjectsId( _nNextFirstId );
                if ( !listIdDataObjectsSublist.isEmpty( ) )
                {
                    _nNextFirstId += _nBatchSize;
                    for ( DataObject obj : _dataSource.getDataObjects( listIdDataObjectsSublist ) )
                    {
                        _mapTmpIdDataObject.put( obj.getId( ), obj );
                    }
                }
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
