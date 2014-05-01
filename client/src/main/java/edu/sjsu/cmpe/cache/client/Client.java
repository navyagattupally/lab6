package edu.sjsu.cmpe.cache.client;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashCode;


public class Client {
	private static HashFunction hashFunction= Hashing.md5();
	//private final int numberOfReplicas=0;
	private static String server1="http://localhost:3000";
	private static String server2="http://localhost:3001";
	private static String server3="http://localhost:3002";

	static char values[]={'a','b','c','d','e','f','g','h','i','j'};




	private static SortedMap<Integer, String> circle =new TreeMap<Integer, String>();


	 public static void add(String node,int i) {
		 HashCode hc=hashFunction.hashLong(i);
		  circle.put(hc.asInt(),node);
	 }
	  public void remove(String node) {

		  //circle.remove(node);
		  circle.remove(Hashing.md5().hashCode());
	  }

	 public static String get(Object key) {
	    if (circle.isEmpty()) {
	      return null;
	    }
	    int hash = hashFunction.hashLong((Integer)key).asInt();
	    if (!circle.containsKey(hash)) {
	      SortedMap<Integer, String> tailMap =circle.tailMap(hash);
	      hash = tailMap.isEmpty() ?
	             circle.firstKey() : tailMap.firstKey();
	    }
	    return circle.get(hash);
	  }

	public static void main(String[] args) throws Exception {
		List<String> servers=new ArrayList<String>();
		servers.add(server1);
		servers.add(server2);
		servers.add(server3);
		System.out.println("Starting Cache Client...");
        /*CacheServiceInterface cache = new DistributedCacheService(
                "http://localhost:3000");

        cache.put(1, "foo");
        System.out.println("put(1 => foo)");

        String value = cache.get(1);
        System.out.println("get(1) => " + value);

        System.out.println("Existing Cache Client...");*/


        for (int i=0;i<servers.size();i++)
        {
        	System.out.println("Server picked: "+ servers.get(i));
        	add(servers.get(i),i);
        }
        for (int i=0;i<10;i++)
        {
        	int bucket = Hashing.consistentHash(Hashing.md5().hashLong(i),circle.size());
        	String cur_server=get(bucket);
        	System.out.println("Present server-->"+cur_server);

        	CacheServiceInterface cache = new DistributedCacheService(cur_server);
        	
        	System.out.println(String.valueOf(values[i]));
        	cache.put(i+1, String.valueOf(values[i]));
        	System.out.println("Value obtained from the server - "+cur_server);
        	cache.get(i+1);
        	System.out.println("Value got-->"+cache.get(i+1));
        	//System.out.println("Cache Client.. ");

        // one of the back end servers is removed from the (middle of the) pool
        	servers.remove(cur_server);
        }

        //servers.remove(cur_server);
    }

}