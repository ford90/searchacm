package com.acm.servlets;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Servlet implementation class DLSearch
 */
@WebServlet("/search")
public class DLSearch extends HttpServlet {
	private static final long serialVersionUID = 1L;
    
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DLSearch() {
        super();
        
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		//Staging box
		//put this inside of a config.properties
		/*
		 * 
		 */
		String index = "acm_20151204";
		/* ***************************** */
		String [] fields = {
			"acmdlTitle^3",
			"acmdlAuthorName^4",
			"acmdlAuthorNameSyn^2",
            "acmdlOtherRolePersonName",
            "recordAbstract^2",
            "keywords.*.keyword^2",
            "_all",
            "recordID",
            "isbnissn.isbnissn",
            "allDOIs.doi",
            "acmdlPublicationTitle.syn",
            "acmdlSponsor",
            "acmdlCCS"
		};

		String[] includes = {  
			"title", 
			"recordID",
			"recordAbstract",
			"ftFormats",
			"publicationDate",
			"publicationTitle",
			"publisherID",
			"publisherName",
			"bibliometric.*",
			"keywords.*",
			"exportFormats.*",
			"owners.owner",
			"parents.publicationTitle",
			"parents.publisherName",
			"parents.publisherID",
			"parents.recordID",
			"parents.title",
			"parents.publicationDate",                              
			"persons.authors.personName",
			"persons.authors.sequence",
			"persons.authors.profileID"                                                                              
		};		


		String query = "java";
		
		QueryBuilder innerQuery = QueryBuilders.multiMatchQuery(query, fields);
		TermFilterBuilder filterQuery = FilterBuilders.termFilter("owners.owner", "ACM");
		
		QueryBuilder qb = QueryBuilders.filteredQuery(innerQuery, filterQuery);
		
		int cnt = 1;
		Client client = (Client)this.getServletContext().getAttribute("elasticClient");
		SearchRequestBuilder reqBuilder = client.prepareSearch(index);
		
		reqBuilder.setTypes("article");
		reqBuilder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		reqBuilder.setQuery(qb);
		reqBuilder.setFetchSource(includes, null);
		reqBuilder.addSort("_score", SortOrder.DESC);
		reqBuilder.setSize(20);
		reqBuilder.setFrom(40);
		
		reqBuilder.setSize(10);

		System.out.println( reqBuilder.toString() );

		client.close();

	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

//	@Override
//	public void init(){
//		String url = "172.16.20.138";
//		Client client = new TransportClient()
//		.addTransportAddress(new InetSocketTransportAddress(url,9300));
//		this.getServletContext().setAttribute("elasticClient", client);
//	}
	
}
