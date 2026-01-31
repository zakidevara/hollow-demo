package com.devara.hollow;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemAnnouncementWatcher;
import com.netflix.hollow.api.consumer.fs.HollowFilesystemBlobRetriever;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class HollowConsumerService {
    private HollowConsumer consumer;
    private final Path publishDir = Paths.get("./hollow-repo");

    @PostConstruct
    public void init() {
        consumer = HollowConsumer.withBlobRetriever(new HollowFilesystemBlobRetriever(publishDir))
                                 .withAnnouncementWatcher(new HollowFilesystemAnnouncementWatcher(publishDir))
                                 .build();
        consumer.triggerRefresh();
    }

    public HollowConsumer getConsumer() {
        return consumer;
    }
}
