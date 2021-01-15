package manoj;

import com.google.protobuf.Empty;
import com.proto.greet.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class GreetingServiceImpl extends GreetServiceGrpc.GreetServiceImplBase {

    private static Set<StreamObserver<GreetEveryoneResponse>> observers = new HashSet<>();

    @Override
    public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
        String firstName = request.getGreeting().getFirstName();
        String result = "Hello " + firstName;
        GreetResponse response = GreetResponse.newBuilder()
                .setResult(result)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void greetManyTimes(GreetManyTimesRequest request, StreamObserver<GreetManyTimesResponse> responseObserver) {
        String firstName = request.getGreeting().getFirstName();
        try {
        for(int i=0;i<10 ;i++){
            String result = "Hello " + firstName + " response number=" + i;
            GreetManyTimesResponse greetManyTimesResponse = GreetManyTimesResponse.newBuilder()
                    .setResult(result)
                    .build();
            responseObserver.onNext(greetManyTimesResponse);

                Thread.sleep(5000L);

        }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            responseObserver.onCompleted();
        }

    }

    @Override
    public StreamObserver<LongGreetRequest> longGreet(StreamObserver<LongGreetResponse> responseObserver) {
        StreamObserver<LongGreetRequest> requestObserver = new StreamObserver<LongGreetRequest>() {

            String result ="";

            @Override
            public void onNext(LongGreetRequest value) {
              //client sends a message
                result += "Hello " + value.getGreeting().getFirstName() + "!";
            }

            @Override
            public void onError(Throwable t) {
                //client sends an error
            }

            @Override
            public void onCompleted() {
               //client is done sending requests
               responseObserver.onNext(LongGreetResponse.newBuilder()
                       .setResult(result)
                       .build());
               responseObserver.onCompleted();

            }
        };
        return requestObserver;
    }

    @Override
    public StreamObserver<GreetEveryoneRequest> greetEveryone(StreamObserver<GreetEveryoneResponse> responseObserver) {

        //observers.add(responseObserver);

        StreamObserver<GreetEveryoneRequest> requestObserver = new StreamObserver<GreetEveryoneRequest>() {
            @Override
            public void onNext(GreetEveryoneRequest value) {
                if(value.getIsadmin()){
                    //first broadcast message to add admin clients
                    String result = "Admin message - " + value.getGreeting().getFirstName();
                    GreetEveryoneResponse greetEveryoneResponse = GreetEveryoneResponse.newBuilder()
                            .setResult(result)
                            .build();
                    for(StreamObserver<GreetEveryoneResponse> observer: observers)
                        observer.onNext(greetEveryoneResponse);


                    //at the end add this admin client  to observers list
                    observers.add(responseObserver);
                }else{
                    String result = "Hello " + value.getGreeting().getFirstName();
                    GreetEveryoneResponse greetEveryoneResponse = GreetEveryoneResponse.newBuilder()
                            .setResult(result)
                            .build();
                    responseObserver.onNext(greetEveryoneResponse);
                }



            }

            @Override
            public void onError(Throwable t) {

                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                        .asException()
                );
            }

            @Override
            public void onCompleted() {
                observers.remove(responseObserver);
               responseObserver.onCompleted();
            }
        };
        return requestObserver;
    }


}
