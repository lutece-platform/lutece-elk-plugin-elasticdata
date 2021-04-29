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

import java.io.Serializable;

/**
 * This is the business class for the object IndexerAction
 */
public class IndexerAction implements Serializable
{
    private static final long serialVersionUID = 1L;

    // Variables declarations
    public static final int TASK_CREATE = 1;
    public static final int TASK_MODIFY = 2;
    public static final int TASK_DELETE = 3;
    private int _nId;
    private String _strIdResource;
    private int _nIdTask;
    private String _strIdDataSource;

    /**
     * Returns the Id
     * 
     * @return The Id
     */
    public int getId( )
    {
        return _nId;
    }

    /**
     * Sets the Id
     * 
     * @param nId
     *            The Id
     */
    public void setId( int nId )
    {
        _nId = nId;
    }

    /**
     * Returns the IdResource
     * 
     * @return The IdResource
     */
    public String getIdResource( )
    {
        return _strIdResource;
    }

    /**
     * Sets the IdResource
     * 
     * @param strIdResource
     *            The IdResource
     */
    public void setIdResource( String strIdResource )
    {
        _strIdResource = strIdResource;
    }

    /**
     * Returns the IdTask
     * 
     * @return The IdTask
     */
    public int getIdTask( )
    {
        return _nIdTask;
    }

    /**
     * Sets the IdTask
     * 
     * @param nIdTask
     *            The IdTask
     */
    public void setIdTask( int nIdTask )
    {
        _nIdTask = nIdTask;
    }

    /**
     * Returns the IdDataSource
     * 
     * @return The IdDataSource
     */
    public String getIdDataSource( )
    {
        return _strIdDataSource;
    }

    /**
     * Sets the IdDataSource
     * 
     * @param strIdDataSource
     *            The IdDataSource
     */
    public void setIdDataSource( String strIdDataSource )
    {
        _strIdDataSource = strIdDataSource;
    }
}
