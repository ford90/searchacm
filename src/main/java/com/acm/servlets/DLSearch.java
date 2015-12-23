package com.acm.servlets;

import java.io.IOException;
import java.util.HashMap;
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
import org.elasticsearch.index.query.RangeFilterBuilder;
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
		
		// Add logic for Query from Wayne's Cold Fusion Code
		
		int size;
		try{
			size = new Integer(request.getParameter("size"));
		} catch(Exception e){
			size = 20;
		}
		int from;
		try{
			from = new Integer(request.getParameter("page"));
			
		} catch(Exception e){
			from = 1;
		}

		from = (from -1) * size;
		
		
		String fullText = request.getParameter("fulltext");
		String filter = request.getParameter("filter");
		int pubYearFrom = 0;
		int pubYearTo = 0;
		try {
			pubYearFrom =  new Integer(request.getParameter("pubyearfrom"));
		} catch(Exception e) {
			pubYearFrom = 0;
		}
		try {
			pubYearTo =  new Integer(request.getParameter("pubyearto"));
		} catch(Exception e) {
			pubYearTo = 0;
		}

		String show = request.getParameter("show");
		String dlnodes = request.getParameter("dlnodes");
		
		
/* ********************* */
		
		
		String result = null;
		
		String includeStr = props.getProperty("includes");
		String fieldStr = props.getProperty("fields");

		QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery(query);

		for(String field : fieldStr.split(",")){
			queryString.field(field);
		}

		Map<String, String> filters = new HashMap<String, String>();
		
		
		if( fullText != null){
			filters.put("fulltext", "ftFormats");
		}

		if(filter != null) {
			
			StringTokenizer filterTokenizer = new StringTokenizer(filter, "|");
			while(filterTokenizer.hasMoreTokens()){
				String filterStr = filterTokenizer.nextToken();
				int pos = filterStr.indexOf(':');
				String term 	 = filterStr.substring(0, pos);
				String value     = filterStr.substring(pos+1).trim();
				
				String oldValue = filters.get(term);
				if(oldValue != null){
					value =  value + "||" + oldValue;
				}
				
				filters.put(term, value);
			}
		}

		if (!filters.containsKey("owners.owner")) {
			filters.put("owners.owner", "ACM");
		}
				
		BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();

		FilterBuilder[] filterArray = new FilterBuilder[filters.size()];

		int cnt = 0;
		for(Map.Entry<String, String> entry : filters.entrySet()){
			String key = entry.getKey();
			FilterBuilder fb = null;
			
			if(key == "fulltext"){
				 fb = FilterBuilders.existsFilter(entry.getValue());
			} else {
				
				String[] termValues = null;
				
				if (entry.getValue().contains("||")) {
					termValues = entry.getValue().split("\\|\\|");
				} else {
					termValues = new String[1];
					termValues[0] = entry.getValue();
				}

				fb = FilterBuilders.termsFilter(key, termValues);
			}
			filterArray[cnt] = fb;
			cnt++;
		}

		boolFilter.must(filterArray);
		
		if (pubYearFrom != 0 || pubYearTo != 0) {
			RangeFilterBuilder rangeFilter = FilterBuilders.rangeFilter("publicationYear");
			if (pubYearFrom != 0) {
				rangeFilter.gte(pubYearFrom);		
			}
			if (pubYearTo != 0) {
				rangeFilter.lte(pubYearTo);		
			}
			boolFilter.must(rangeFilter);
		}

		// Complete Query
		QueryBuilder qb = QueryBuilders.filteredQuery( queryString, boolFilter);

		Client client = (Client)this.getServletContext().getAttribute("elasticClient");

		SearchRequestBuilder reqBuilder = client.prepareSearch(props.getProperty("index"));

		reqBuilder.setTypes("article");
		reqBuilder.setSearchType(SearchType.DFS_QUERY_AND_FETCH);
		reqBuilder.setQuery(qb);
		reqBuilder.setFetchSource(includeStr.split(","), null);
		reqBuilder.addSort("_score", SortOrder.DESC);
		reqBuilder.setSize(size);
		reqBuilder.setFrom(from);		


		
		if (show != null) {
//			response.getWriter().print(reqBuilder.toString());
			result = reqBuilder.toString();
		} else {
			SearchResponse elasticResponse = reqBuilder.execute().actionGet();
			//if (dlnodes != null) {
				//getDLNodes(elasticResponse);
			//}
			result = elasticResponse.toString();
//			response.getWriter().print(elasticResponse.toString());
		}
		response.getWriter().print(result);
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


