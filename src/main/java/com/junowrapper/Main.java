package com.junowrapper;

import com.paypal.juno.client.io.JunoRequest;
import com.paypal.juno.client.io.JunoResponse;
import com.paypal.juno.client.io.OperationStatus;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {

    public static void bulkAdd(int total, int batchSize, TimeUnit timeUnit, long timeToLive, Function<Integer, String> valueFunction) {
        Set<String> response = new HashSet<>();
        int count = 0;

        for (int i = 1; i <= total; i += batchSize) {
            List<JunoRequest> batch = IntStream.rangeClosed(i, Math.min(i + batchSize - 1, total))
                    .boxed()
                    .map(j -> JunoDBManager.setJunoRequest(j, valueFunction.apply(j), JunoRequest.OperationType.Set, timeUnit.toSeconds(timeToLive)))
                    .collect(Collectors.toList());

            Iterable<JunoResponse> junoResponses = JunoDBManager.doBatch(batch);

            for (JunoResponse junoResponse : Objects.requireNonNull(junoResponses)) {
                if (junoResponse.getStatus() == OperationStatus.Success) {
                    count++;
                } else {
                    response.add(junoResponse.getStatus().getErrorText());
                }
            }
        }

        System.out.println(response);
        System.out.println("Total processed: " + count);
    }

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
//        bulkAdd(2000, 2000, TimeUnit.SECONDS, 60, (i -> "sdad" + i));
    }
}
