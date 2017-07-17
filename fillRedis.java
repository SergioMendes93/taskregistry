import redis.clients.jedis.Jedis; 
import redis.clients.jedis.Pipeline; 

public class fillRedis { 
   public static void main(String[] args) { 


        //TODO o porto vai vir no request enviado pelo utilizador

      //Connecting to Redis server on localhost , para fazer requests em vez de localhost vai ser o IP do host que esta a correr
      Jedis jedis = new Jedis(args[0], Integer.parseInt(args[1])); 
      //set the data in redis string 
//      jedis.set("tutorialname", "Redis tutorial"); 
      // Get the stored data and print it 
  //    System.out.println("Stored string in redis:: "+ jedis.get("tutorialname")); */

        /*
                Este i vai depender da memoria pedida, por exemplo 3x este valor da 5gb RAM portanto
        */
        long fiveMega = 524288000; //500mb
        long correspondentValue = 2097151; //Value to get 500mb being consumed
        long memoryRequirement = Long.parseLong(args[2],10); 

        //TODO este codigo pode estar no task registry, o memory requirement vai ser substituido pelo valor de memoria pedido pelo user
        //TODO, nao encher tudo o que o utilizador pedir, pensar sobre isto. por exemplo se ele pedir 4gb, encher 3gb? 2.5gb? falar com o prof sobre isto.
        //TODO os requests vai ser tudo a mesma key para simplificar

        long value = (memoryRequirement * correspondentValue) / fiveMega; //TODO ver se isto funciona para 4gb (valor maximo)

        Pipeline pipeline = jedis.pipelined();
        for (long i = 0 ; i < value; i++) {
                pipeline.sadd("key"+i, "value");
        // you can call pipeline.sync() and start new pipeline here if you think there're so much operations in one pipeline
	        pipeline.sync();
        }
   } 
}

