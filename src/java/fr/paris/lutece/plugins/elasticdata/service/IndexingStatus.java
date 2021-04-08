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

import java.util.concurrent.atomic.AtomicBoolean;

public class IndexingStatus
{
    private int _nNbTotalObj;
    private int _nCurrentNbIndexedObj;  
    private AtomicBoolean _bIsRunning = new AtomicBoolean( );
    private StringBuilder _sbLogs;
    
    
    /**
     * Get the total number of data objects to index
     * 
     * @return the total number or data objects to index
     */
    public int getNbTotalObj( )
    {
        return _nNbTotalObj;
    }

    /**
     * Set the total number of data objects to index
     * 
     * @param nNbTotalObj
     *            The total number of objs
     */
    public void setnNbTotalObj( int nNbTotalObj )
    {
        _nNbTotalObj = nNbTotalObj;
    }

    /**
     * Get the current number of indexed objects
     * 
     * @return the current number of indexed objects
     */
    public int getCurrentNbIndexedObj( )
    {
        return _nCurrentNbIndexedObj;
    }

    /**
     * Set the current number of indexed objects
     * 
     * @param nCurrentNbIndexedObj
     *            the current
     * 
     */
    public void setCurrentNbIndexedObj( int nCurrentNbIndexedObj )
    {
        _nCurrentNbIndexedObj = nCurrentNbIndexedObj;
    }
    /**
     * Returns the IsRunning
     * @return The IsRunning
     */ 
     public AtomicBoolean getIsRunning()
     {
         return _bIsRunning;
     }
 
    /**
     * Sets the IsRunning
     * @param bIsRunning The IsRunning
     */ 
     public void setIsRunning( AtomicBoolean bIsRunning )
     {
         _bIsRunning = bIsRunning;
     }

    /**
     * Get the percent of indexed objects
     * 
     * @return the percent of the indexed objects
     */
    public double getProgress( )
    {
    	if(_nNbTotalObj == 0) {
    		
    		return 0;
    	}
        return (double) _nCurrentNbIndexedObj / (double) _nNbTotalObj * 100.0;
    }
    /**
     * Returns the SbLogs
     * @return The SbLogs
     */ 
     public String getStringSbLogs()
     {
    	 if( _sbLogs == null )
    	 {
    		 _sbLogs =new StringBuilder( ); 
    	 }
         return _sbLogs.toString( );
     } 
    /**
     * Returns the SbLogs
     * @return The SbLogs
     */ 
     public StringBuilder getSbLogs()
     {
    	 if( _sbLogs == null )
    	 {
    		 _sbLogs =new StringBuilder( ); 
    	 }
         return _sbLogs;
     } 
    /**
     * Sets the SbLogs
     * @param sbLogs The SbLogs
     */ 
     public void setSbLogs( StringBuilder sbLogs )
     {
         _sbLogs = sbLogs;
     }
    /**
     * Reset the Indexing Status
     */
    public void reset() {
    	
    	_nNbTotalObj= 0;
    	_nCurrentNbIndexedObj= 0;
    	if( _sbLogs != null )
    	{
    		_sbLogs.setLength( 0 );
    	}else {
    		
    		 _sbLogs =new StringBuilder( );
    	}
    	
    }
}
