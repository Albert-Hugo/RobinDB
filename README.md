# RobinDB
SSTable implement in Java


### usage
    mvn clean package 

After build success , then distribution package will be generated.

To start the startup script under RobinDB-{version}/startup directory

#### basic command

Open browser , visit ip:8888, **post** a request to put a key value

    curl -H "Content-Type:application/json" -X POST -d '{"key":"Hello","val":"RobinDB"}' 'http://ip:8888/put'

Then get the result  , using a **get** request

    curl 'http://ip:8888/get/{key}'
    
To remove a key , using **delete** request

    curl -X DELETE 'http://ip:8888/delete/{key}'
    
#### Using only SSTable embeded in pure java code

Include maven dependency

    <dependency>
        <groupId>com.ido.robin</groupId>
        <artifactId>core</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
            

Then create a SSTable instance to manage content

    SSTable ssTable = new SSTable(".");
    ssTable.put("test","client");
    ssTable.flush();
    System.out.println(ssTable.get("test"));
    ssTable.close();
    
#### Support Add and Remove server node dynamically

##### To add a new node to serve 

    curl -H "Content-Type:application/json" -X POST -d '{"host":"new-host","port":"server-port","httpPort":"the-server-port""}' 'http://ip:8888/node/add'
    
after the command return success response , then a new server is added to serve.

##### To remove a existed node

And send blow command to remove a serving node

    curl -H "Content-Type:application/json" -X DELETE -d '{"host":"new-host","port":"server-port"}' 'http://ip:8888/node/delete'
    




  
 