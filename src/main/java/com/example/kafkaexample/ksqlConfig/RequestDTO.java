package com.example.kafkaexample.ksqlConfig;

public class RequestDTO {
    private String profileId;
    private double latitude;
    private double longiTude;

    public String getProfileId() {
        return profileId;
    }

    public void setProfileId(String profileId) {
        this.profileId = profileId;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongiTude() {
        return longiTude;
    }

    public void setLongiTude(double longiTude) {
        this.longiTude = longiTude;
    }
}
