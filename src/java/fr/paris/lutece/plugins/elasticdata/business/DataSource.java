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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * DataSource the dataObject type
 */
public interface DataSource
{

    /**
     * The Data Source Id
     *
     * @return The Id
     */
    String getId( );

    /**
     * The Data Source name
     *
     * @return The name
     */
    String getName( );

    /**
     * The target index name to be created
     *
     * @return The index name
     */
    String getTargetIndexName( );

    /**
     * Get the business id of all the data objects to fetch
     * 
     * @return collection of id of all the data objects of the datasource
     */
    List<String> getIdDataObjects( );

    /**
     * Get the full data objects from given list of ids
     * 
     * @param listIdDataObjects
     * @return the collection of the data objects corresponsing to given list of id objects.
     */
    List<DataObject> getDataObjects( List<String> listIdDataObjects );

    /**
     * An iterator of data object
     *
     * @return an Iterator
     */

    Iterator<DataObject> getDataObjectsIterator( );

    /**
     * Returns the BatchSize
     *
     * @return The BatchSize
     */
    int getBatchSize( );

    /**
     * Get specific mappings for the Data Source
     * 
     * @return The mappings as JSON
     */
    String getMappings( );

    /**
     * Contains Geo Point
     * 
     * @return true if localizable
     */
    boolean isLocalizable( );

    /**
     * Uses the full indexing daemon
     * 
     * @return true if the datasource should be fully indexed by the daemon
     */
    boolean usesFullIndexingDaemon( );

    /**
     * Get the external providers of attributes for DataSources
     * 
     * @return the list of DataSourceAttributesProvider
     */
    Collection<IDataSourceExternalAttributesProvider> getExternalAttributesProvider( );

}
