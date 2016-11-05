package com.stanton.article.reader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.JSONObject;

public class ArticleReader {
	private static final String ARTICLE_FILE = "/home/ross/WebstormProjects/ElasticProduct/article.json";
	private static Client client;
	private static WebTarget target;
	
	public static void main(String[] args) throws Exception{
		client = ClientBuilder.newClient();
		target = client.target("http://localhost:9200").path("/products/product");
		
		readFile();
	}
	
	private static JSONObject parse(byte[] data){
		String d = new String(data);
		try{
			JSONObject json = new JSONObject(d);
			return json;
		}
		catch(Exception e){
			System.out.println("Couldn't Parse "+d);
			return null;
		}
	}
	
	private static void index(JSONObject data){
		try{
			String id = data.getString("ean");
			WebTarget targ = target.path(id);
			
			Invocation.Builder builder = targ.request("application/json");
			Response r = builder.post(Entity.entity(data.toString(), MediaType.APPLICATION_JSON));
			
			if(r.getStatus()>201){
				System.out.println("Error Writing to Elastic Search for EAN "+id);
			}
			
		}
		catch(Exception e){
			System.out.println(e.toString());
		}
	}
	
	private static void readFile() throws Exception{
		//read a byte
		//if byte is { start a new buffer
		//if byte is } end buffer and convert
		File articleFile = new File(ARTICLE_FILE);
		
		ByteArrayOutputStream bos = null;
		
		FileInputStream fis = new FileInputStream(articleFile);
		int counter = 0;
		
		while(fis.available()>0){
			int c = fis.read();
		
			if(c==123){ //{
				bos = new ByteArrayOutputStream();
				counter++;
			}
			if(c==125){ //}
				bos.write(c);
				
				//parse
				JSONObject obj = parse(bos.toByteArray());
				
				//write to Elastic Search
				if(obj!=null)
					index(obj);
			}
			else{
				bos.write(c);
			}
		}
		System.out.println("Read "+counter);
	}
}
