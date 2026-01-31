package com.devara.hollow;

import com.devara.hollow.model.UserAccount;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer;
import com.netflix.hollow.api.producer.fs.HollowFilesystemPublisher;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Service
public class HollowProducerService {
    private HollowProducer producer;
    private final Path publishDir = Paths.get("./hollow-repo");

    @PostConstruct
    public void init() {
        java.io.File dir = publishDir.toFile();
        if (!dir.exists()) dir.mkdirs();

        producer = HollowProducer.withPublisher(new HollowFilesystemPublisher(publishDir))
                                 .withAnnouncer(new HollowFilesystemAnnouncer(publishDir))
                                 .build();
    }

    public void runCycle(List<UserAccount> data) {
        producer.runCycle(state -> {
            for (UserAccount user : data) {
                state.add(user);
            }
        });
    }
}
