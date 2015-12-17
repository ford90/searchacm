package com.acm.servlets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

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
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.acm.ReadProperties;



/**
 * Servlet implementation class DLSearch
 */
@WebServlet("/search")
public class DLSearch extends HttpServlet {
	private static final long serialVersionUID = 1L;
    
	private ReadProperties props;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DLSearch() {
        super();
        this.props = ReadProperties.getInstance();
        
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
/* ********* GET PARAMETERS ******* */
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

		String owner = request.getParameter("owner");
		String fullText = request.getParameter("fulltext");
		
		String filter = request.getParameter("filter");
		
/* ********************* */
		
		String includeStr = props.getProperty("includes");
		String fieldStr = props.getProperty("fields");

		QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery(query);

		for(String field : fieldStr.split(",")){
			queryString.field(field);
		}

		Map<String, String> filters = new HashMap<String, String>();

		if(owner != null ) {
			filters.put("owners.owner", "ACM");
		}
		else {
			// Default Behavior
			filters.put("owners.owner", "GUIDE");
		}

		if( fullText != null){
			filters.put("fulltext", "ftFormats");
		}

		if(filter != null) {
			
			StringTokenizer filterTokenizer = new StringTokenizer(filter, "|");
			while(filterTokenizer.hasMoreTokens()){
				String filterStr = filterTokenizer.nextToken();
				String term 	 = filter.substring(0, filterStr.indexOf(':'));
				String value 	 = filter.substring(filterStr.indexOf("(")+1,filter.indexOf(")"));

				filters.put(term, value);
			}
		}

		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();

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
		boolFilter.must(filterArray);

		// Complete Query
		QueryBuilder qb = QueryBuilders.filteredQuery( queryString, boolFilter);

		Client client = (Client)this.getServletContext().getAttribute("elasticClient");

		SearchRequestBuilder reqBuilder = client.prepareSearch(props.getProperty("index"));

		reqBuilder.setTypes("article");
		reqBuilder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		reqBuilder.setQuery(qb);
		reqBuilder.setFetchSource(includeStr.split(","), null);
		reqBuilder.addSort("_score", SortOrder.DESC);
		reqBuilder.setSize(20);
		reqBuilder.setFrom(0);		

		/*    
		SearchResponse elasticResponse = reqBuilder.execute().actionGet();

		ArrayList<String> recordID_l = new ArrayList<String>();

		for( SearchHit hit : elasticResponse.getHits().getHits() ){
//			Map<String,Object> source = hit.getSource();
			recordID_l.add(hit.getId());
			
		}

		for(String recordID : recordID_l){
			out.write(recordID.getBytes());
			out.write("\n".getBytes());
			
		}
		*/
		response.getWriter().print(reqBuilder.toString());
		response.getWriter().flush();
		
//		SearchHit[] hits = elasticSearchResults(query, size, from, owner, filter, fullText );
		
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
	
	private SearchHit[] elasticSearchResults(String query, int size, int from, String owner, String filter, String fullText ){

		String includeStr = props.getProperty("includes");
		String fieldStr = props.getProperty("fields");

		QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery(query);

		for(String field : fieldStr.split(",")){
			queryString.field(field);
		}
		


		Map<String, String> filters = new HashMap<String, String>();

		if(owner != null ) {
			filters.put("owners.owner", "ACM");
		}
		else {
			// Default Behavior
			filters.put("owners.owner", "GUIDE");
		}

		if( fullText != null){
			filters.put("fulltext", "ftFormats");
		}

		if(filter != null) {
			
			StringTokenizer filterTokenizer = new StringTokenizer(filter, "|");
			while(filterTokenizer.hasMoreTokens()){
				String filterStr = filterTokenizer.nextToken();
				String term 	 = filter.substring(0, filterStr.indexOf(':'));
				String value 	 = filter.substring(filterStr.indexOf("(")+1,filter.indexOf(")"));

				filters.put(term, value);
			}
		}

		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();

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
		boolFilter.must(filterArray);

		// Complete Query
		QueryBuilder qb = QueryBuilders.filteredQuery( queryString, boolFilter);

		Client client = (Client)this.getServletContext().getAttribute("elasticClient");

		SearchRequestBuilder reqBuilder = client.prepareSearch(props.getProperty("index"));

		reqBuilder.setTypes("article");
		reqBuilder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		reqBuilder.setQuery(qb);
		reqBuilder.setFetchSource(includeStr.split(","), null);
		reqBuilder.addSort("_score", SortOrder.DESC);
		reqBuilder.setSize(20);
		reqBuilder.setFrom(0);		
		    
		SearchResponse elasticResponse = reqBuilder.execute().actionGet();
		 
		
		return elasticResponse.getHits().getHits();
	}
	
	
}
