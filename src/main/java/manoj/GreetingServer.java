package manoj;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class GreetingServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Hello grpc");
        Server server = ServerBuilder.forPort(50051)
                .addService(new GreetingServiceImpl())
                .build();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Recieved shoutdown hook");
            server.shutdown();
            System.out.println("Succeesfully stopped the server");
        }));
        server.awaitTermination();
    }
}
