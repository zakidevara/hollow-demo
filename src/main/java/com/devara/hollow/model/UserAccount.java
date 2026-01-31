package com.devara.hollow.model;

import com.netflix.hollow.core.write.objectmapper.HollowPrimaryKey;

import lombok.Data;

@HollowPrimaryKey(fields="id")
@Data
public class UserAccount {
    private long id;
    private String username;
    private boolean active;

    // Default constructor required for Jackson deserialization
    public UserAccount() {
    }

    // Standard constructor, getters, and setters
    public UserAccount(long id, String username, boolean active) {
        this.id = id;
        this.username = username;
        this.active = active;
    }
}