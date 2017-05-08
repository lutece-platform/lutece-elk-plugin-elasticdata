<jsp:useBean id="manageelasticdata" scope="session" class="fr.paris.lutece.plugins.elasticdata.web.ManageElasticDataJspBean" />
<% String strContent = manageelasticdata.processController ( request , response ); %>

<%@ page errorPage="../../ErrorPage.jsp" %>
<jsp:include page="../../AdminHeader.jsp" />

<%= strContent %>

<%@ include file="../../AdminFooter.jsp" %>

