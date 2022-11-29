package com.example.kafkaexample.ksqlConfig;

public class ResponseDTO {

    private String profileId;
    private String latitude;
    private String longItude;

    public ResponseDTO() {

    }

    public String getProfileId() {
        return profileId;
    }

    public void setProfileId(String profileId) {
        this.profileId = profileId;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongItude() {
        return longItude;
    }

    public void setLongItude(String longItude) {
        this.longItude = longItude;
    }
}
