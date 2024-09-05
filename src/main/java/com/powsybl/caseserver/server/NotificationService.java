package com.powsybl.caseserver.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {
    private static final String CATEGORY_BROKER_OUTPUT = CaseService.class.getName() + ".output-broker-messages";
    private static final Logger OUTPUT_MESSAGE_LOGGER = LoggerFactory.getLogger(CATEGORY_BROKER_OUTPUT);

    @Autowired
    private StreamBridge caseInfosPublisher;

    public void sendImportMessage(Message<String> message) {
        OUTPUT_MESSAGE_LOGGER.debug("Sending message : {}", message);
        caseInfosPublisher.send("publishCaseImport-out-0", message);
    }
}
