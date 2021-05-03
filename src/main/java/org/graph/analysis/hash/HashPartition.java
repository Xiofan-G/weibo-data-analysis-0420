package org.graph.analysis.hash;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HashPartition implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        Map<String, Integer> ipMap = this.buildNodeIp(numPartitions);
        ConsistentHashing hashService = new ConsistentHashing(ipMap.keySet());

        String targetIp = hashService.getObjectNode(key);

        return ipMap.get(targetIp);
    }

    private Map<String, Integer> buildNodeIp(int numPartitions) {
        Map<String, Integer> ipMap = new HashMap<>();
        Random random = new Random();
        for (int i = 0; i < numPartitions; i++) {
            String ip = String.format(
                    "%d.%d.%d.%d",
                    random.nextInt(255) % (255) + 1,
                    random.nextInt(255) % (255) + 1,
                    random.nextInt(255) % (255) + 1,
                    random.nextInt(255) % (255) + 1);
            while (ipMap.containsKey(ip)) {
                ip = String.format(
                        "%d.%d.%d.%d",
                        random.nextInt(255) % (255) + 1,
                        random.nextInt(255) % (255) + 1,
                        random.nextInt(255) % (255) + 1,
                        random.nextInt(255) % (255) + 1);
            }

            ipMap.put(ip, i);
        }
        return ipMap;
    }
}