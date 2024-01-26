package com.junowrapper;

import com.junowrapper.juno.JunoDBManager;
import com.junowrapper.juno.collection.JunoMap;
import com.junowrapper.juno.model.JunoDBConfig;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Main {

    public static void bulkAdd(int total, int batchSize, TimeUnit timeUnit, long timeToLive, Function<Integer, String> valueFunction) {
//        Set<String> response = new HashSet<>();
//        int count = 0;
//
//        for (int i = 1; i <= total; i += batchSize) {
//            List<JunoRequest> batch = IntStream.rangeClosed(i, Math.min(i + batchSize - 1, total))
//                    .boxed()
//                    .map(j -> JunoDBManager.setJunoRequest(j, valueFunction.apply(j), JunoRequest.OperationType.Set, timeUnit.toSeconds(timeToLive)))
//                    .collect(Collectors.toList());
//
//            Iterable<JunoResponse> junoResponses = JunoDBManager.doBatch(batch);
//
//            for (JunoResponse junoResponse : Objects.requireNonNull(junoResponses)) {
//                if (junoResponse.getStatus() == OperationStatus.Success) {
//                    count++;
//                } else {
//                    response.add(junoResponse.getStatus().getErrorText());
//                }
//            }
//        }
//
//        System.out.println(response);
//        System.out.println("Total processed: " + count);
    }

    public static void main(String[] args) {
        JunoDBConfig junoDBConfig = new JunoDBConfig("127.0.0.1", 8080, "ShowYTest", "junocache");
        JunoMap<String, Integer> junoMap = new JunoMap<>("DSEWEEE", new JunoDBManager(junoDBConfig));

        System.out.println(junoMap.put("421", 245));
        Integer x = junoMap.get("421");
        System.out.println(x);

//        long startTime = System.currentTimeMillis();
//        bulkAdd(2000, 2000, TimeUnit.SECONDS, 60, (i -> "sdad" + i));
    }
}
