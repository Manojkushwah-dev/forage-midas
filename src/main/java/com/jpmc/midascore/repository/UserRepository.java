package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface UserRepository extends CrudRepository<UserRecord, Long> {
    Optional<UserRecord> findById(Long id); // ✅ built-in method

    UserRecord findByName(String name);


}
