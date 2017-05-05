<%@ page errorPage="../../ErrorPage.jsp" %>

<jsp:include page="../../AdminHeader.jsp" />

<jsp:useBean id="manageelasticdata" scope="session" class="fr.paris.lutece.plugins.elasticdata.web.ManageElasticDataJspBean" />

<% manageelasticdata.init( request, manageelasticdata.RIGHT_MANAGEELASTICDATA ); %>
<%= manageelasticdata.getManageElasticDataHome ( request ) %>

<%@ include file="../../AdminFooter.jsp" %>
