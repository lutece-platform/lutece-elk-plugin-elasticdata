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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import fr.paris.lutece.plugins.elasticdata.service.IndexingStatus;


/**
 * AbstractDataSource
 */
public abstract class AbstractDataSource implements DataSource
{


    // Variables declarations
    private String _strId;
    private String _strName;
    private String _strTargetIndexName;
    private int _nBatchSize = DataSource.BATCH_SIZE;
    private String _strMappings;
    private boolean _bLocalizable;
    private boolean _bFullIndexingDaemon;
    private IndexingStatus _indexingStatus;
    protected Collection<IDataSourceExternalAttributesProvider> _colExternalAttributesProvider;

    /**
     * Returns the Id
     *
     * @return The Id
     */
    @Override
    public String getId( )
    {
        return _strId;
    }

    /**
     * Sets the Id
     *
     * @param strId
     *            The Id
     */
    public void setId( String strId )
    {
        _strId = strId;
    }

    /**
     * Returns the Name
     *
     * @return The Name
     */
    @Override
    public String getName( )
    {
        return _strName;
    }

    /**
     * Sets the Name
     *
     * @param strName
     *            The Name
     */
    public void setName( String strName )
    {
        _strName = strName;
    }

    /**
     * Returns the TargetIndexName
     *
     * @return The TargetIndexName
     */
    @Override
    public String getTargetIndexName( )
    {
        return _strTargetIndexName;
    }

    /**
     * Sets the TargetIndexName
     *
     * @param strTargetIndexName
     *            The TargetIndexName
     */
    public void setTargetIndexName( String strTargetIndexName )
    {
        _strTargetIndexName = strTargetIndexName;
    }

    /**
     * Returns the BatchSize
     * 
     * @return The BatchSize
     */
    @Override
    public int getBatchSize( )
    {
        return _nBatchSize;
    }

    /**
     * Sets the BatchSize
     * 
     * @param nBatchSize
     *            The BatchSize
     */
    public void setBatchSize( int nBatchSize )
    {
        _nBatchSize = nBatchSize;
    }

    /**
     * Returns the Localizable
     * 
     * @return The Localizable
     */
    @Override
    public boolean isLocalizable( )
    {
        return _bLocalizable;
    }

    /**
     * Sets the Localizable
     * 
     * @param bLocalizable
     *            The Localizable
     */
    public void setLocalizable( boolean bLocalizable )
    {
        _bLocalizable = bLocalizable;
    }

    /**
     * Returns the Mappings
     * 
     * @return The Mappings
     */
    @Override
    public String getMappings( )
    {
        return _strMappings;
    }

    /**
     * Sets the Mappings
     * 
     * @param strMappings
     *            The Mappings
     */
    public void setMappings( String strMappings )
    {
        _strMappings = strMappings;
    }

    /**
     * Returns the FullIndexingDaemon
     * 
     * @return The FullIndexingDaemon
     */
    @Override
    public boolean usesFullIndexingDaemon( )
    {
        return _bFullIndexingDaemon;
    }

    /**
     * Sets the FullIndexingDaemon
     * 
     * @param bFullIndexingDaemon
     *            The FullIndexingDaemon
     */
    public void setFullIndexingDaemon( boolean bFullIndexingDaemon )
    {
        _bFullIndexingDaemon = bFullIndexingDaemon;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<DataObject> getDataObjectsIterator( )
    {
    	List<String> listIdDataObject= this.getIdDataObjects( );
        this.getIndexingStatus().setnNbTotalObj(listIdDataObject.size( ));

        return new BatchDataObjectsIterator( this, listIdDataObject );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<DataObject> getDataObjectsIterator( List<String> listIdDataObjects )
    {
        return new BatchDataObjectsIterator( this, listIdDataObjects );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IDataSourceExternalAttributesProvider> getExternalAttributesProvider( )
    {
        if ( _colExternalAttributesProvider == null )
        {
            _colExternalAttributesProvider = new ArrayList<>( );
        }
        return _colExternalAttributesProvider;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public  IndexingStatus getIndexingStatus( ) {
    	
    	if( _indexingStatus == null ) {
    		
    		_indexingStatus= new IndexingStatus( );
    	}
    	return _indexingStatus ;
    }
    /**
     * Set the external attributes provider for the data source
     * 
     * @param colExternalAttributesProvider
     */
    public void setExternalAttributesProvider( Collection<IDataSourceExternalAttributesProvider> colExternalAttributesProvider )
    {
        _colExternalAttributesProvider = colExternalAttributesProvider;
    }
    
}
