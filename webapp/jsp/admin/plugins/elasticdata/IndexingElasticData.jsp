<jsp:useBean id="manageelasticdata" scope="session" class="fr.paris.lutece.plugins.elasticdata.web.ManageElasticDataJspBean" />
<% String strContent = manageelasticdata.processController ( request , response ); %>
<%= strContent %>
