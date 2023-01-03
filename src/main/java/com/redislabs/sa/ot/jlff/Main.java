package com.redislabs.sa.ot.jlff;

import com.google.gson.Gson;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.json.JSONObject;
import redis.clients.jedis.*;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.providers.PooledConnectionProvider;

import java.io.BufferedReader;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * This class loads JSON objects into RedisJSON from a specified file
 * The source file will be in ~ separated format.
 * Column 1 will be keyname, column 2 will be JSON payload
 * KEYNAME    |   JSON
 * jkey123:abc | {"name":"Ralph","species":"Canine","times": [{"military": "0800","civilian": "8 AM"}]}
 * To invoke locate the file of interest and provide the path to it like this:
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.21 --port 12500 --filepath /Users/owentaylor/wip/java/JSONLoaderFromFile1/src/main/resources/jsonkeyvalue.tldf --pipebatchsize 12"
 */

public class Main {
    /**
     */

    static String host = "localhost";
    static int port = 6379;
    static String username = "default";
    static String password = "";
    static String filepath = "";
    static ConnectionHelper connectionHelper = null;
    static int pipeBatchSize = 200;

    public static void main(String[] args) {

        if (args.length > 0) {
            ArrayList<String> argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--host")) {
                int index = argList.indexOf("--host");
                host = argList.get(index + 1);
            }
            if (argList.contains("--port")) {
                int index = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(index + 1));
            }
            if (argList.contains("--user")) {
                int index = argList.indexOf("--user");
                username = argList.get(index + 1);
            }
            if (argList.contains("--password")) {
                int index = argList.indexOf("--password");
                password = argList.get(index + 1);
            }
            if (argList.contains("--filepath")) {
                int index = argList.indexOf("--filepath");
                filepath = argList.get(index + 1);
            }
            if (argList.contains("--pipebatchsize")) {
                int index = argList.indexOf("--pipebatchsize");
                pipeBatchSize = Integer.parseInt(argList.get(index + 1));
            }
        }
        try{
            connectionHelper = new ConnectionHelper(ConnectionHelper.buildURI(host,port,username,password));
            loadJSONDataFromFile(filepath,connectionHelper);
            manipulateJsonData(connectionHelper);
        }catch(Throwable t){t.printStackTrace();}

    }

    static int loadJSONDataFromFile(String path,ConnectionHelper connectionHelper) throws Throwable{
        BufferedReader reader = new BufferedReader(Files.newBufferedReader(Paths.get(path)));
        Gson gson = new Gson();
        int pipeCounter =0;
        Pipeline pipeline = connectionHelper.getPipeline();
        String s = null;
        do {
            s = reader.readLine();
            if(null!=s) {
                String[] lineRead = s.split("~");
                if (pipeCounter % 1000 == 0) {
                    System.out.println("key == " + lineRead[0]);//# DEBUG
                    System.out.println("json == " + lineRead[1]);//# DEBUG
                }
                String json = lineRead[1];
                Map<?, ?> map = gson.fromJson(json, Map.class);
                JSONObject obj = new JSONObject(map);
                pipeline.jsonSet(lineRead[0], obj);
                pipeCounter++;
                if (pipeCounter % 1000 == 0) {
                    System.out.println("obj added to pipeline...\n" + obj); //# DEBUG
                }
                if (pipeCounter % pipeBatchSize == 0) {
                    pipeline.sync();
                }
            }
        }while(null!=s);
        pipeline.sync();//in case there are extra objects in the pipe
        return pipeCounter;
    }

    static void manipulateJsonData(ConnectionHelper connectionHelper){
        Gson gson = new Gson();
        Pipeline pipeline = connectionHelper.getPipeline();
        //***  dcp:{batch1}1070, dcp:{batch1}1407 are objects from the test dataset file
        pipeline.jsonDel("dcp:{batch1}1070", Path2.of("$.columndetails[2].columnid"));
        pipeline.jsonSet("dcp:{batch1}1070",Path2.of("$.buname"),gson.toJson("FIN"));
        pipeline.jsonSet("dcp:{batch1}1407",Path2.of("$.buname"),gson.toJson("FINAL"));
        pipeline.sync();
        System.out.println("Pipeline complete");
        //*** now as a TX:
        Transaction tx = connectionHelper.getTransaction();
        tx.jsonDel("dcp:{batch1}1070", Path2.of("$.columndetails[3].columnid"));
        tx.jsonSet("dcp:{batch1}1070", Path2.of("$.columndetails[2].columnid"),gson.toJson(99888776));
        tx.jsonSet("dcp:{batch1}1070",Path2.of("$.buname"),gson.toJson("AGGORAL"));
        tx.jsonSet("dcp:{batch1}1407",Path2.of("$.buname"),gson.toJson("BORRAYS"));
        tx.exec();
        System.out.println("TX complete");
    }
}


class ConnectionHelper{

    final PooledConnectionProvider connectionProvider;
    final JedisPooled jedisPooled;

    /**
     * Used when you want to send a batch of commands to the Redis Server
     * @return Pipeline
     */
    public Pipeline getPipeline(){
        return  new Pipeline(jedisPooled.getPool().getResource());
    }

    /**
     * Assuming use of Jedis 4.3.1:
     * https://github.com/redis/jedis/blob/82f286b4d1441cf15e32cc629c66b5c9caa0f286/src/main/java/redis/clients/jedis/Transaction.java#L22-L23
     * @return Transaction
     */
    public Transaction getTransaction(){
        return new Transaction(jedisPooled.getPool().getResource());
    }

    /**
     * Obtain the default object used to perform Redis commands
     * @return JedisPooled
     */
    public JedisPooled getPooledJedis(){
        return jedisPooled;
    }

    /**
     * Use this to build the URI expected in this classes' Constructor
     * @param host
     * @param port
     * @param username
     * @param password
     * @return
     */
    public static URI buildURI(String host,int port,String username,String password){
        URI uri = null;
        try {
            if (!("".equalsIgnoreCase(password))) {
                uri = new URI("redis://" + username + ":" + password + "@" + host + ":" + port);
            } else {
                uri = new URI("redis://" + host + ":" + port);
            }
        } catch (URISyntaxException use) {
            use.printStackTrace();
            System.exit(1);
        }
        return uri;
    }


    public ConnectionHelper(URI uri){
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        JedisClientConfig clientConfig = null;
        System.out.println("$$$ "+uri.getAuthority().split(":").length);
        if(uri.getAuthority().split(":").length==3){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1];
            password = password.split("@")[0];
            System.out.println("\n\nUsing user: "+user+" / password @@@@@@@@@@"+password);
            clientConfig = DefaultJedisClientConfig.builder().user(user).password(password)
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings

        }else {
            clientConfig = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings
        }
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(1000);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWait(Duration.ofMinutes(1));
        poolConfig.setTestOnCreate(true);

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
    }
}
