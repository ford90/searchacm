package com.acm.servlets;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;



/**
 * Servlet implementation class DLSearch
 */
@WebServlet("/search")
public class DLSearch extends HttpServlet {
	private static final long serialVersionUID = 1L;
    
//	private ReadProperties props;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DLSearch() {
        super();
//        this.props = ReadProperties.getInstance();
        
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
		String query = request.getParameter("q");
		int size;
		try{
			size = new Integer(request.getParameter("size"));
		} catch(Exception e){
			size = 0;
		}
		int from;
		try{
			from = new Integer(request.getParameter("offset"));
		} catch(Exception e){
			from = 0;
		}
		
		
		String index = "acm_20151204";
		/* ***************************** */
	/* */
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
	
/* */
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


//		String query = "Moshe Vardi";
		
		QueryBuilder qsb = QueryBuilders
				.queryStringQuery(query)
				.field("acmdlTitle^3")
				.field("acmdlAuthorName^4" )
				.field("acmdlAuthorNameSyn^2")
				.field("acmdlOtherRolePersonName")
				.field("recordAbstract^2" )
				.field("keywords.*.keyword^2" )
				.field("_all")
				.field("recordID")
				.field("isbnissn.isbnissn")
				.field("allDOIs.doi")
				.field("acmdlPublicationTitle.syn")
				.field("acmdlSponsor")
				.field("acmdlCCS");

//		QueryStringQueryBuilder qsb = QueryBuilders.queryStringQuery("");
		
		/*
		String fieldStr = props.getProperty("fields");
		
		String includeStr = props.getProperty("includes");
		

		
		StringTokenizer includeTokenizer = new StringTokenizer(includeStr, ",");
		String[] includes = new String[includeTokenizer.countTokens()];
		
		int cnt = 0;
		while(includeTokenizer.hasMoreTokens()){
			includes[cnt] = includeTokenizer.nextToken();
			cnt++;
		}
		*/
		String owner = request.getParameter("owner");

		
		
		Map<String, String> filters = new HashMap<String, String>();
		

		if(owner != null ) {
			filters.put("owners.owner", "ACM");
		}
		else {
			// Default Behavior
			filters.put("owners.owner", "GUIDE");
		}
		
		String personName = request.getParameter("personName");
		
		if( personName != null){
			filters.put("acmdlPersonsSearchDspName", personName);
		}
		
		String fullText = request.getParameter("fulltext");
		if( fullText != null){
			filters.put("fulltext", "ftFormats");
		}
		
		
		
		BoolFilterBuilder bfq = FilterBuilders.boolFilter();
//		  .must(FilterBuilders.termFilter("owners.owner", "ACM"),
//				  FilterBuilders.termFilter("acmdlPersonsSearchDspName", "Binder, Walter"));
		
		FilterBuilder[] filterArray = new FilterBuilder[filters.size()];
		int cnt = 0;
		for(Map.Entry<String, String> entry : filters.entrySet()){
			String key = entry.getKey();
			FilterBuilder fb = null;
			if(key == "fulltext"){
				 fb = FilterBuilders.existsFilter(entry.getValue());
			} else{
				fb = FilterBuilders.termFilter(key, entry.getValue());
			}
			filterArray[cnt] = fb;
			cnt++;

		}
		bfq.must(filterArray);
//		System.out.println(fieldStr);
//		QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(q, fields);
//		QueryBuilder qb2 = QueryBuilders.boolQuery()
//				.should(queryBuilder);
		
		QueryBuilder qb = QueryBuilders.filteredQuery(
								qsb,
								bfq);
		

		
//		StringTokenizer fieldTokenizer = new StringTokenizer(fieldStr, ",");
		
//		while(fieldTokenizer.hasMoreTokens()){
//			String field = fieldTokenizer.nextToken();
//			qsb.field(field);
//		}

//		QueryBuilder innerQuery = QueryBuilders.multiMatchQuery(query, fields);
		//TermFilterBuilder filterQuery = FilterBuilders.termFilter("owners.owner", "ACM");
		
//		AndFilterBuilder filterQuery = FilterBuilders.andFilter(FilterBuilders.termFilter("owners,owener", "GUIDE"),
//				FilterBuilders.termFilter("owners.owner", "ACM"));
		
//		QueryBuilder qb = QueryBuilders.filteredQuery(innerQuery, filterQuery);
		
		
		
		Client client = (Client)this.getServletContext().getAttribute("elasticClient");
		SearchRequestBuilder reqBuilder = client.prepareSearch();
		
		reqBuilder.setTypes("article");
		reqBuilder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		reqBuilder.setQuery(qb);
		reqBuilder.setFetchSource(includes, null);
		reqBuilder.addSort("_score", SortOrder.DESC);
		reqBuilder.setSize(20);
		reqBuilder.setFrom(0);
		

		System.out.println( reqBuilder.toString() );

		/*    
		SearchResponse elasticResponse = reqBuilder.execute().actionGet();


				
		ArrayList<String> recordID_l = new ArrayList<String>();
		
		for( SearchHit hit : elasticResponse.getHits().getHits() ){
//			Map<String,Object> source = hit.getSource();
			recordID_l.add(hit.getId());
			
		}
		
		OutputStream out = response.getOutputStream();
		
		for(String recordID : recordID_l){
			out.write(recordID.getBytes());
			out.write("\n".getBytes());
			
		}
		*/
		OutputStream out = response.getOutputStream();
		out.write("Hello".getBytes());
		out.flush();
		
//		client.close();

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
	
	private SearchHit[] elasticSearchResults(String query, int size, int from ){
		
//		Old way of doing it, now use properties file to loop through and add field
//		
		
		
//		ACMQueryBuilder
		
		QueryBuilder qsb = QueryBuilders
				.queryStringQuery(query)
				.field("acmdlTitle^3")
				.field("acmdlAuthorName^4" )
				.field("acmdlAuthorNameSyn^2")
				.field("acmdlOtherRolePersonName")
				.field("recordAbstract^2" )
				.field("keywords.*.keyword^2" )
				.field("_all")
				.field("recordID")
				.field("isbnissn.isbnissn")
				.field("allDOIs.doi")
				.field("acmdlPublicationTitle.syn")
				.field("acmdlSponsor")
				.field("acmdlCCS");

//		QueryStringQueryBuilder qsb = QueryBuilders.queryStringQuery("");
		
		/*
		String fieldStr = props.getProperty("fields");
		
		String includeStr = props.getProperty("includes");
		

		
		StringTokenizer includeTokenizer = new StringTokenizer(includeStr, ",");
		String[] includes = new String[includeTokenizer.countTokens()];
		
		int cnt = 0;
		while(includeTokenizer.hasMoreTokens()){
			includes[cnt] = includeTokenizer.nextToken();
			cnt++;
		}
		*/
//		System.out.println(fieldStr);
//		QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(q, fields);
//		QueryBuilder qb2 = QueryBuilders.boolQuery()
//				.should(queryBuilder);
		
		QueryBuilder qb = QueryBuilders.filteredQuery(
								qsb,
								FilterBuilders.termFilter("owners.owner", "ACM"));
		
//		StringTokenizer fieldTokenizer = new StringTokenizer(fieldStr, ",");
		
//		while(fieldTokenizer.hasMoreTokens()){
//			String field = fieldTokenizer.nextToken();
//			qsb.field(field);
//		}

//		QueryBuilder innerQuery = QueryBuilders.multiMatchQuery(query, fields);
		//TermFilterBuilder filterQuery = FilterBuilders.termFilter("owners.owner", "ACM");
		
//		AndFilterBuilder filterQuery = FilterBuilders.andFilter(FilterBuilders.termFilter("owners,owener", "GUIDE"),
//				FilterBuilders.termFilter("owners.owner", "ACM"));
		
//		QueryBuilder qb = QueryBuilders.filteredQuery(innerQuery, filterQuery);
		
		
		
		Client client = (Client)this.getServletContext().getAttribute("elasticClient");
		SearchRequestBuilder reqBuilder = client.prepareSearch();
		
		reqBuilder.setTypes("article");
		reqBuilder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		reqBuilder.setQuery(qb);
//		reqBuilder.setFetchSource(includes, null);
		reqBuilder.addSort("_score", SortOrder.DESC);
		reqBuilder.setSize(20);
		reqBuilder.setFrom(0);
		

		System.out.println( reqBuilder.toString() );

		/* */   
		SearchResponse elasticResponse = reqBuilder.execute().actionGet();

		
		return elasticResponse.getHits().getHits();
	}
	
	
}
