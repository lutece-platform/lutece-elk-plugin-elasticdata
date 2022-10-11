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
package fr.paris.lutece.plugins.elasticdata.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.paris.lutece.plugins.elasticdata.business.DataSource;
import fr.paris.lutece.plugins.elasticdata.service.DataSourceIncrementalService;
import fr.paris.lutece.plugins.elasticdata.service.DataSourceService;
import fr.paris.lutece.plugins.libraryelastic.util.ElasticClientException;
import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.portal.util.mvc.admin.annotations.Controller;
import fr.paris.lutece.portal.util.mvc.commons.annotations.Action;
import fr.paris.lutece.portal.util.mvc.commons.annotations.View;
import fr.paris.lutece.portal.service.message.AdminMessage;
import fr.paris.lutece.portal.service.message.AdminMessageService;
import fr.paris.lutece.portal.service.security.SecurityTokenService;
import fr.paris.lutece.plugins.elasticdata.business.IndexerAction;
import fr.paris.lutece.plugins.elasticdata.business.IndexerActionHome;
import fr.paris.lutece.portal.service.admin.AccessDeniedException;
import fr.paris.lutece.portal.service.util.AppException;
import fr.paris.lutece.util.url.UrlItem;
import fr.paris.lutece.util.html.AbstractPaginator;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;

/**
 * ManageElasticData JSP Bean abstract class for JSP Bean
 */
@Controller( controllerJsp = "ManageElasticData.jsp", controllerPath = "jsp/admin/plugins/elasticdata/", right = "ELASTICDATA_MANAGEMENT" )
public class ManageElasticDataJspBean extends AbstractManageJspBean <Integer, IndexerAction>
{
    private static final long serialVersionUID = 1L;

    // Templates
    private static final String TEMPLATE_MANAGE_INDEXERACTIONS = "/admin/plugins/elasticdata/manage_indexeractions.html";
    private static final String TEMPLATE_CREATE_INDEXERACTION = "/admin/plugins/elasticdata/create_indexeraction.html";
    private static final String TEMPLATE_MODIFY_INDEXERACTION = "/admin/plugins/elasticdata/modify_indexeraction.html";
    private static final String TEMPLATE_HOME = "/admin/plugins/elasticdata/manage_elasticdata.html";

    // Parameters
    private static final String PARAMETER_ID_INDEXERACTION = "id";
    private static final String PARAMETER_DATA_SOURCE = "data_source";

    // Properties for page titles
    private static final String PROPERTY_PAGE_TITLE_MANAGE_INDEXERACTIONS = "elasticdata.manage_indexeractions.pageTitle";
    private static final String PROPERTY_PAGE_TITLE_MODIFY_INDEXERACTION = "elasticdata.modify_indexeraction.pageTitle";
    private static final String PROPERTY_PAGE_TITLE_CREATE_INDEXERACTION = "elasticdata.create_indexeraction.pageTitle";
    private static final String PROPERTY_PAGE_TITLE = "elasticdata.manage_elasticdata.title";

    // Markers
    private static final String MARK_INDEXERACTION_LIST = "indexeraction_list";
    private static final String MARK_INDEXERACTION = "indexeraction";
    private static final String MARK_DATA_SOURCES_LIST = "data_sources_list";

    private static final String JSP_MANAGE_INDEXERACTIONS = "jsp/admin/plugins/elasticdata/ManageElasticData.jsp";

    // Properties
    private static final String MESSAGE_CONFIRM_REMOVE_INDEXERACTION = "elasticdata.message.confirmRemoveIndexerAction";

    // Validations
    private static final String VALIDATION_ATTRIBUTES_PREFIX = "elasticdata.model.entity.indexeraction.attribute.";

    // Views
    private static final String VIEW_MANAGE_INDEXERACTIONS = "manageIndexerActions";
    private static final String VIEW_CREATE_INDEXERACTION = "createIndexerAction";
    private static final String VIEW_MODIFY_INDEXERACTION = "modifyIndexerAction";
    private static final String VIEW_HOME = "home";
    private static final String VIEW_INCREMENTAL = "incremental";

    // Actions
    private static final String ACTION_CREATE_INDEXERACTION = "createIndexerAction";
    private static final String ACTION_MODIFY_INDEXERACTION = "modifyIndexerAction";
    private static final String ACTION_REMOVE_INDEXERACTION = "removeIndexerAction";
    private static final String ACTION_CONFIRM_REMOVE_INDEXERACTION = "confirmRemoveIndexerAction";
    private static final String ACTION_CHECK_INDEX_STATUS = "checkIndexStatus";
    private static final String ACTION_INDEX = "index";
    private static final String ACTION_INDEX_INCREMENTAL = "index_incremental";

    // Infos
    private static final String INFO_INDEXERACTION_CREATED = "elasticdata.info.indexeraction.created";
    private static final String INFO_INDEXERACTION_UPDATED = "elasticdata.info.indexeraction.updated";
    private static final String INFO_INDEXERACTION_REMOVED = "elasticdata.info.indexeraction.removed";
    
    // Errors
    private static final String ERROR_RESOURCE_NOT_FOUND = "Resource not found";
    
    // Session variable to store working values
    private IndexerAction _indexeraction;
    private List<Integer> _listIdIndexerActions;

    ObjectMapper _mapper = new ObjectMapper( );

    /**
     * View the home of the feature
     * 
     * @param request
     *            The HTTP request
     * @return The page
     */
    @View( value = VIEW_HOME, defaultView = true )
    public String getManageElasticData( HttpServletRequest request )
    {
        Map<String, Object> model = getModel( );
        model.put( MARK_DATA_SOURCES_LIST, DataSourceService.getDataSources( ) );

        return getPage( PROPERTY_PAGE_TITLE, TEMPLATE_HOME, model );
    }

        /**
     * View the home of the feature
     * 
     * @param request
     *            The HTTP request
     * @return The page
     */
    @View( value = VIEW_INCREMENTAL )
    public String getManageElasticDataIncremental( HttpServletRequest request )
    {
        Map<String, Object> model = getModel( );
        model.put( MARK_DATA_SOURCES_LIST, DataSourceService.getDataSources( ) );

        return getPage( PROPERTY_PAGE_TITLE, TEMPLATE_HOME, model );
    }

    /**
     * Process the full indexing of a given data source
     * 
     * @param request
     *            The HTTP request
     * @return The redirected page
     */
    @Action( ACTION_INDEX )
    public String doIndex( HttpServletRequest request ) throws ElasticClientException
    {
        String strDataSourceId = request.getParameter( PARAMETER_DATA_SOURCE );
        DataSource dataSource = DataSourceService.getDataSource( strDataSourceId );
        DataSourceService.processFullIndexing( dataSource, true );

        return redirect( request, VIEW_HOME );

    }

    /**
     * Process the incremental indexing of a given data source
     * 
     * @param request
     *            The HTTP request
     * @return The redirected page
     */
    @Action( ACTION_INDEX_INCREMENTAL )
    public String doIncrementalIndexing( HttpServletRequest request ) throws ElasticClientException
    {
        String strDataSourceId = request.getParameter( PARAMETER_DATA_SOURCE );
        DataSource dataSource = DataSourceService.getDataSource( strDataSourceId );
        DataSourceIncrementalService.processAsynchronouslyIncrementalIndexing( dataSource );

        return redirect( request, VIEW_HOME );
    }

    /**
     *
     * @param request
     * @return
     * @throws ElasticClientException
     */
    @Action( ACTION_CHECK_INDEX_STATUS )
    public String doCheckIndexStatus( HttpServletRequest request ) throws ElasticClientException
    {
        String strDataSourceId = request.getParameter( PARAMETER_DATA_SOURCE );
        return getJsonStatus( strDataSourceId );
    }

    /**
     * Get Json status of given data source id
     * 
     * @param strDataSourceId
     * @return JsonStatus of given datasource id
     */
    private String getJsonStatus( String strDataSourceId )
    {
        try
        {
            return _mapper.writeValueAsString( DataSourceService.getDataSource( strDataSourceId ).getIndexingStatus( ) );
        }
        catch( JsonProcessingException e )
        {
            AppLogService.error( "Unable to serialize index status", e );
            return StringUtils.EMPTY;
        }
    }

       /**
     * Build the Manage View
     * @param request The HTTP request
     * @return The page
     */
    @View( value = VIEW_MANAGE_INDEXERACTIONS )
    public String getManageIndexerActions( HttpServletRequest request )
    {
        _indexeraction = null;
        
        if ( request.getParameter( AbstractPaginator.PARAMETER_PAGE_INDEX) == null || _listIdIndexerActions.isEmpty( ) )
        {
        	_listIdIndexerActions = IndexerActionHome.getIdIndexerActionsList(  );
        }
        
        Map<String, Object> model = getPaginatedListModel( request, MARK_INDEXERACTION_LIST, _listIdIndexerActions, JSP_MANAGE_INDEXERACTIONS );

        return getPage( PROPERTY_PAGE_TITLE_MANAGE_INDEXERACTIONS, TEMPLATE_MANAGE_INDEXERACTIONS, model );
    }

	/**
     * Get Items from Ids list
     * @param listIds
     * @return the populated list of items corresponding to the id List
     */
	@Override
	List<IndexerAction> getItemsFromIds( List<Integer> listIds ) 
	{
		List<IndexerAction> listIndexerAction = IndexerActionHome.getIndexerActionsListByIds( listIds );
		
		// keep original order
        return listIndexerAction.stream()
                 .sorted(Comparator.comparingInt( notif -> listIds.indexOf( notif.getId())))
                 .collect(Collectors.toList());
	}
    
    /**
    * reset the _listIdIndexerActions list
    */
    public void resetListId( )
    {
    	_listIdIndexerActions = new ArrayList<>( );
    }

    /**
     * Returns the form to create a indexeraction
     *
     * @param request The Http request
     * @return the html code of the indexeraction form
     */
    @View( VIEW_CREATE_INDEXERACTION )
    public String getCreateIndexerAction( HttpServletRequest request )
    {
        _indexeraction = ( _indexeraction != null ) ? _indexeraction : new IndexerAction(  );

        Map<String, Object> model = getModel(  );
        model.put( MARK_INDEXERACTION, _indexeraction );
        model.put( SecurityTokenService.MARK_TOKEN, SecurityTokenService.getInstance( ).getToken( request, ACTION_CREATE_INDEXERACTION ) );

        return getPage( PROPERTY_PAGE_TITLE_CREATE_INDEXERACTION, TEMPLATE_CREATE_INDEXERACTION, model );
    }

    /**
     * Process the data capture form of a new indexeraction
     *
     * @param request The Http Request
     * @return The Jsp URL of the process result
     * @throws AccessDeniedException
     */
    @Action( ACTION_CREATE_INDEXERACTION )
    public String doCreateIndexerAction( HttpServletRequest request ) throws AccessDeniedException
    {
        populate( _indexeraction, request, getLocale( ) );
        

        if ( !SecurityTokenService.getInstance( ).validate( request, ACTION_CREATE_INDEXERACTION ) )
        {
            throw new AccessDeniedException ( "Invalid security token" );
        }

        // Check constraints
        if ( !validateBean( _indexeraction, VALIDATION_ATTRIBUTES_PREFIX ) )
        {
            return redirectView( request, VIEW_CREATE_INDEXERACTION );
        }

        IndexerActionHome.create( _indexeraction );
        addInfo( INFO_INDEXERACTION_CREATED, getLocale(  ) );
        resetListId( );

        return redirectView( request, VIEW_MANAGE_INDEXERACTIONS );
    }

    /**
     * Manages the removal form of a indexeraction whose identifier is in the http
     * request
     *
     * @param request The Http request
     * @return the html code to confirm
     */
    @Action( ACTION_CONFIRM_REMOVE_INDEXERACTION )
    public String getConfirmRemoveIndexerAction( HttpServletRequest request )
    {
        int nId = Integer.parseInt( request.getParameter( PARAMETER_ID_INDEXERACTION ) );
        UrlItem url = new UrlItem( getActionUrl( ACTION_REMOVE_INDEXERACTION ) );
        url.addParameter( PARAMETER_ID_INDEXERACTION, nId );

        String strMessageUrl = AdminMessageService.getMessageUrl( request, MESSAGE_CONFIRM_REMOVE_INDEXERACTION, url.getUrl(  ), AdminMessage.TYPE_CONFIRMATION );

        return redirect( request, strMessageUrl );
    }

    /**
     * Handles the removal form of a indexeraction
     *
     * @param request The Http request
     * @return the jsp URL to display the form to manage indexeractions
     */
    @Action( ACTION_REMOVE_INDEXERACTION )
    public String doRemoveIndexerAction( HttpServletRequest request )
    {
        int nId = Integer.parseInt( request.getParameter( PARAMETER_ID_INDEXERACTION ) );
        
        
        IndexerActionHome.remove( nId );
        addInfo( INFO_INDEXERACTION_REMOVED, getLocale(  ) );
        resetListId( );

        return redirectView( request, VIEW_MANAGE_INDEXERACTIONS );
    }

    /**
     * Returns the form to update info about a indexeraction
     *
     * @param request The Http request
     * @return The HTML form to update info
     */
    @View( VIEW_MODIFY_INDEXERACTION )
    public String getModifyIndexerAction( HttpServletRequest request )
    {
        int nId = Integer.parseInt( request.getParameter( PARAMETER_ID_INDEXERACTION ) );

        if ( _indexeraction == null || ( _indexeraction.getId(  ) != nId ) )
        {
            Optional<IndexerAction> optIndexerAction = IndexerActionHome.findByPrimaryKey( nId );
            _indexeraction = optIndexerAction.orElseThrow( ( ) -> new AppException(ERROR_RESOURCE_NOT_FOUND ) );
        }


        Map<String, Object> model = getModel(  );
        model.put( MARK_INDEXERACTION, _indexeraction );
        model.put( SecurityTokenService.MARK_TOKEN, SecurityTokenService.getInstance( ).getToken( request, ACTION_MODIFY_INDEXERACTION ) );

        return getPage( PROPERTY_PAGE_TITLE_MODIFY_INDEXERACTION, TEMPLATE_MODIFY_INDEXERACTION, model );
    }

    /**
     * Process the change form of a indexeraction
     *
     * @param request The Http request
     * @return The Jsp URL of the process result
     * @throws AccessDeniedException
     */
    @Action( ACTION_MODIFY_INDEXERACTION )
    public String doModifyIndexerAction( HttpServletRequest request ) throws AccessDeniedException
    {   
        populate( _indexeraction, request, getLocale( ) );
		
		
        if ( !SecurityTokenService.getInstance( ).validate( request, ACTION_MODIFY_INDEXERACTION ) )
        {
            throw new AccessDeniedException ( "Invalid security token" );
        }

        // Check constraints
        if ( !validateBean( _indexeraction, VALIDATION_ATTRIBUTES_PREFIX ) )
        {
            return redirect( request, VIEW_MODIFY_INDEXERACTION, PARAMETER_ID_INDEXERACTION, _indexeraction.getId( ) );
        }

        IndexerActionHome.update( _indexeraction );
        addInfo( INFO_INDEXERACTION_UPDATED, getLocale(  ) );
        resetListId( );

        return redirectView( request, VIEW_MANAGE_INDEXERACTIONS );
    }

}
