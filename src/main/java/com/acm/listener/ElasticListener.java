package com.acm.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;

/**
 * Application Lifecycle Listener implementation class ElasticListener
 *
 */
public class ElasticListener implements ServletContextListener {

    /**
     * Default constructor. 
     */
    public ElasticListener() {
        // TODO Auto-generated constructor stub
    }

	/**
     * @see ServletContextListener#contextDestroyed(ServletContextEvent)
     */
    public void contextDestroyed(ServletContextEvent arg0)  { 
    	 Client client = (Client)arg0.getServletContext().getAttribute("elasticClient");
    	 client.close();
         System.out.println("ServletContextDestoryed");
    }

	/**
     * @see ServletContextListener#contextInitialized(ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent arg0)  { 
         System.out.println("ServletContextCreated");
 		String url = "172.16.20.19";
//        String url = "acmelasticstge.acm.org";
         
//        String url = "http://acmelasticdev.priv.acm.org";
 		Client client = new TransportClient()
 		.addTransportAddress(new InetSocketTransportAddress(url,9300));
        
//        Client client = NodeBuilder.nodeBuilder().client(true).node().client();
        
 		arg0.getServletContext().setAttribute("elasticClient", client);
    }
	
}
