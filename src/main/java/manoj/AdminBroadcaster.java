package manoj;

import com.google.common.util.concurrent.ListenableFuture;
import com.proto.greet.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AdminBroadcaster {

    public static void main(String[] args) {
        System.out.println("Hello Grpc client");
        //DummyServiceGrpc.DummyServiceBlockingStub syncClient = DummyServiceGrpc.newBlockingStub(channel);
        AdminBroadcaster main = new AdminBroadcaster();
        main.run();
    }

    private void run(){
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        //doUnaryCall(channel);
        //doServerStreamingCall(channel);
        //doClientStreamingCall(channel);
        doBiDiStreaminCall(channel);
        System.out.println("Shutting down channel");
        channel.shutdown();

    }



    private void doUnaryCall(ManagedChannel channel) {
        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);
        GreetServiceGrpc.GreetServiceFutureStub future = GreetServiceGrpc.newFutureStub(channel);
        // Add listener to future response to get actual result
        ListenableFuture<GreetResponse> futureResponse = future.greet(GreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("FutureStub")
                        .build())
                .build());


        Greeting greeting = Greeting.newBuilder()
                .setFirstName("Manoj")
                .setLastName("Pardeshi")
                .build();
        GreetRequest greetRequest = GreetRequest.newBuilder()
                .setGreeting(greeting)
                .build();
        GreetResponse response = greetClient.greet(greetRequest);
        System.out.println("Response:- " + response.getResult());
    }

    private void doServerStreamingCall(ManagedChannel channel) {
        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

        // Server Streaming
        // we prepare the request
        GreetManyTimesRequest greetManyTimesRequest =
                GreetManyTimesRequest.newBuilder()
                        .setGreeting(Greeting.newBuilder().setFirstName("Manoj"))
                        .build();

        // we stream the responses (in a blocking manner)
        greetClient.greetManyTimes(greetManyTimesRequest)
                .forEachRemaining(greetManyTimesResponse -> {
                    System.out.println(greetManyTimesResponse.getResult());

                });
        System.out.println("Finished client processing");

    }

    private void doClientStreamingCall(ManagedChannel channel) {
        //create async stub, blocking stub wont work
        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        //When we get the server response , these methods will get called
        StreamObserver<LongGreetRequest> requestObserver =  asyncClient.longGreet(new StreamObserver<LongGreetResponse>() {
            @Override
            public void onNext(LongGreetResponse value) {
                //when we get final response from the server
                System.out.println("Recieved a response from server");
                System.out.println(value.getResult());
                //This will be called only once
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                //server is finish sending the response.
                latch.countDown();
            }
        });

        System.out.println("Sending Message #1");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("Manoj")
                        .build())
                .build());

        System.out.println("Sending Message #2");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("John")
                        .build())
                .build());

        System.out.println("Sending Message #3");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("Mark")
                        .build())
                .build());
        //we will say to the server thast client has done sendding the data
        requestObserver.onCompleted();
        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private void doBiDiStreaminCall(ManagedChannel channel) {

        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<GreetEveryoneRequest> requestObserver = asyncClient.greetEveryone(new StreamObserver<GreetEveryoneResponse>() {
            @Override
            public void onNext(GreetEveryoneResponse value) {
                System.out.println("Response from server: " + value.getResult());
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();  
            }

            @Override
            public void onCompleted() {
                System.out.println("Server is done sending data");
                latch.countDown();
            }
        });

        Arrays.asList("Message1", "Message2", "Message3", "Message4").forEach(
                name -> {
                    //System.out.println("Sending: " + name + " ClientTwo");
                    requestObserver.onNext(GreetEveryoneRequest.newBuilder()
                            .setGreeting(Greeting.newBuilder()
                                    .setFirstName(name))
                            .setIsadmin(true)
                            .build());
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
        );

        requestObserver.onCompleted();

        try {
            latch.await(40, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
