/*
 * Copyright (c) 2002-2018, Mairie de Paris
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

public class IndexingStatus 
{
    private String _strIndexId;
    private int _nNbTotalObj;
    private int _nCurrentNbIndexedObj;

    /**
     * Get the index id
     * @return the index id
     */
    public String getIndexId() {
        return _strIndexId;
    }

    /**
     * Set the index id
     * @param strIndexId the index id 
     */
    public void setIndexId( String strIndexId ) 
    {
        _strIndexId = strIndexId;
    }

    /**
     * Get the total number of data objects to index
     * @return the total number or data objects to index
     */
    public int getNbTotalObj() {
        return _nNbTotalObj;
    }

    /**
     * Set the total number of data objects to index
     * @param nNbTotalObj 
     *              The total number of objs
     */
    public void setnNbTotalObj( int nNbTotalObj ) 
    {
        _nNbTotalObj = nNbTotalObj;
    }

    /**
     * Get the current number of indexed objects
     * @return the current number of indexed objects
     */
    public int getCurrentNbIndexedObj() 
    {
        return _nCurrentNbIndexedObj;
    }

    /**
     * Set the current number of indexed objects
     * @param nCurrentNbIndexedObj 
     *     the current
     *              
     */
    public void setCurrentNbIndexedObj( int nCurrentNbIndexedObj ) 
    {
        _nCurrentNbIndexedObj = nCurrentNbIndexedObj;
    }
    
    /**
     * Get the percent of indexed objects
     * @return the percent of the indexed objects
     */
    public double getPercent( )
    {
        double dPercent = (double)_nCurrentNbIndexedObj / (double)_nNbTotalObj * 100.0;
        return  dPercent;
    }
}
