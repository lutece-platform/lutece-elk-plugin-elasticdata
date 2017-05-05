/*
 * Copyright (c) 2002-2017, Mairie de Paris
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

import fr.paris.lutece.plugins.elasticdata.business.DataObject;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.libraryelastic.util.Elastic;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.spring.SpringContextService;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DataSourceService
 */
public class DataSourceService
{
    public List<DataSource> getDataSources()
    {
        return SpringContextService.getBeansOfType( DataSource.class );
    }

    public void insertData( StringBuilder sbLogs, DataSource dataSource , boolean bReset ) throws ElasticClientException
    {
        Elastic elastic = new Elastic();
        if( bReset )
        {
            elastic.deleteIndex( dataSource.getTargetIndexName() );
            elastic.createMappings( dataSource.getTargetIndexName(), getTimestampMappings( dataSource.getDataType() ));
        }
        Collection<DataObject> listDataObjects = dataSource.getDataObjects();
        for( DataObject object : listDataObjects )
        {
            elastic.create( dataSource.getTargetIndexName(), dataSource.getDataType(), object );
        }
        sbLogs.append( "Number of object inserted for Data Source '" ).append( dataSource.getName() ).append( "' : " ).append(listDataObjects.size());
    }
    
    
    public String insertDataAllDatasources( boolean bReset ) throws ElasticClientException
    {
        StringBuilder sbLogs = new StringBuilder();
        
        for( DataSource dataSource : getDataSources() )
        {
            insertData( sbLogs , dataSource, bReset );
        }
        return sbLogs.toString();
    }
    

    private String getTimestampMappings( String strType )
    {
        return "{ \"mappings\": { \"" + strType + "\" : { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"x\" }}}}}";
    }
}
