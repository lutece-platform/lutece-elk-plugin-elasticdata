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

import fr.paris.lutece.portal.web.l10n.LocaleService;

import java.util.Calendar;
import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * AbstractDataObject
 */

public abstract class AbstractDataObject implements DataObject
{

    private String _strId;
    private long _lTimestamp;
    private String _strDayOfWeek;
    private String _strMonth;
    private String _strHour;
    private String _strPrefixedDayOfWeek;
    private String _strPrefixedMonth;
    private String _strParentId;
    private String _strParentName;
    private String _strDocumentTypeName;

    /**
     * {@inheritDoc }
     */
    @Override
    public String getTimestamp( )
    {
        return String.valueOf( _lTimestamp );
    }

    /**
     * Set the notification timestamp
     * 
     * @param lTimestamp
     *            the notification timestamp
     */
    public final void setTimestamp( long lTimestamp )
    {
        Locale locale = LocaleService.getDefault( );
        _lTimestamp = lTimestamp;
        Calendar calendar = Calendar.getInstance( locale );
        calendar.setTimeInMillis( lTimestamp );
        _strDayOfWeek = calendar.getDisplayName( Calendar.DAY_OF_WEEK, Calendar.LONG, locale );
        _strPrefixedDayOfWeek = ( ( ( calendar.get( Calendar.DAY_OF_WEEK ) + 5 ) % 7 ) + 1 ) + " - " + _strDayOfWeek;
        _strMonth = calendar.getDisplayName( Calendar.MONTH, Calendar.LONG, locale );
        _strPrefixedMonth = String.format( "%02d", calendar.get( Calendar.MONTH ) + 1 ) + " - " + _strMonth;
        _strHour = String.format( "%02d", calendar.get( Calendar.HOUR_OF_DAY ) );
    }

    @JsonIgnore
    @Override
    public String getId( )
    {

        return _strId;
    }

    public void setId( String id )
    {
        _strId = id;
    }

    @Override
    public String getParentId( )
    {
        return _strParentId;
    }

    public void setParentId( String parentId )
    {
        _strParentId = parentId;
    }

    @Override
    public String getParentName( )
    {
        return _strParentName;
    }

    public void setParentName( String parentName )
    {
        _strParentName = parentName;
    }

    @Override
    public String getDocumentTypeName( )
    {
        return _strDocumentTypeName;
    }

    public void setDocumentTypeName( String documentTypeName )
    {
        _strDocumentTypeName = documentTypeName;
    }

    /**
     * Returns the day of week
     * 
     * @return the day of week
     */
    public String getDayOfWeek( )
    {
        return _strDayOfWeek;
    }

    /**
     * Returns the month
     * 
     * @return the month
     */
    public String getMonth( )
    {
        return _strMonth;
    }

    /**
     * Returns the Hour
     * 
     * @return the Hour
     */
    public String getHour( )
    {
        return _strHour;
    }

    /**
     * Returns the day of week prefixed by the day number
     * 
     * @return the day of week prefixed by the day number
     */
    public String getPrefixedDayOfWeek( )
    {
        return _strPrefixedDayOfWeek;
    }

    /**
     * Returns the month prefixed by the day number
     * 
     * @return the month prefixed by the day number
     */
    public String getPrefixedMonth( )
    {
        return _strPrefixedMonth;
    }

}
