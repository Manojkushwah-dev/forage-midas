package com.jpmc.midascore.api;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    private static final Logger logger = LoggerFactory.getLogger(BalanceController.class);
    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam("userId") long userId) {
        Balance balance = userRepository.findById(userId)
                .map(user -> new Balance(user.getBalance()))
                .orElse(new Balance(0));

        logger.info("Balance requested for userId {}: {}", userId, balance.getAmount());
        return balance;
    }
}


