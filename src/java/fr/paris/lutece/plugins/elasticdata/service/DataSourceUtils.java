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

import fr.paris.lutece.portal.service.plugin.Plugin;
import fr.paris.lutece.portal.service.plugin.PluginService;
import fr.paris.lutece.portal.service.util.AppPropertiesService;

public class DataSourceUtils
{

    // Build a JSON mappings block to declare 'timestamp' field as a date
    public static final String TIMESTAMP_MAPPINGS = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\" }}}}";
    // Build a JSON mappings block to declare 'timestamp' field as a date and 'location' field as a geo_point
    public static final String TIMESTAMP_AND_LOCATION_MAPPINGS = "{ \"mappings\":  { \"properties\": { \"timestamp\": { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis\" }, \"location\": { \"type\": \"geo_point\"} } }}}";

    public static final String PLUGIN_NAME = "elasticdata";
    public static final String INSTANCE_NAME = AppPropertiesService.getProperty( "lutece.name" );
    public static final String PREFIX_DATA_OBJECT_ID = INSTANCE_NAME + "_";
    public static final String RESOURCE_TYPE_INDEXING = "ELASTICDATA_DATASOURCE_INDEXING";

    private DataSourceUtils( )
    {
        throw new IllegalStateException( "Utility class" );
    }

    /**
     * Gets the elasticdata plugin
     * 
     * @return the plugin
     */
    public static Plugin getPlugin( )
    {
        return PluginService.getPlugin( PLUGIN_NAME );
    }

}
