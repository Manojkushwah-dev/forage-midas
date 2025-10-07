package com.jpmc.midascore.kafka;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository,
                               TransactionRecordRepository transactionRecordRepository,
                               RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        UserRecord sender = userRepository.findById(senderId).orElse(null);
        UserRecord recipient = userRepository.findById(recipientId).orElse(null);

        if (sender == null || recipient == null) {
            return; // Invalid user
        }

        if (sender.getBalance() < amount) {
            return; // Insufficient funds
        }

        // ✅ Call Incentive API
        Incentive incentive = new Incentive(0);
        try {
            incentive = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
        } catch (Exception e) {
            // Log or handle if needed
        }

        float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0f;

        // ✅ Adjust balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        // ✅ Save users
        userRepository.save(sender);
        userRepository.save(recipient);

        // ✅ Save transaction record with incentive
        TransactionRecord record = new TransactionRecord(sender, recipient, amount, incentiveAmount);
        transactionRecordRepository.save(record);
    }
}

