package com.zqg.kakfautils;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.Date;

public class Log  implements Serializable {


    private String userId;
    private Date  accessTime;
    private String  province;
    private String  city;
    private String  AdvId;
    public String getUserId() {
        return userId;
    }
    public void setUserId(String userId) {
        this.userId = userId;
    }
    public Date getAccessTime() {
        return accessTime;
    }
    public void setAccessTime(Date accessTime) {
        this.accessTime = accessTime;
    }
    public String getProvince() {
        return province;
    }
    public void setProvince(String province) {
        this.province = province;
    }
    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
    public String getAdvId() {
        return AdvId;
    }
    public void setAdvId(String advId) {
        AdvId = advId;
    }
    public Log(String userId, Date accessTime, String province, String city, String advId) {
        super();
        this.userId = userId;
        this.accessTime = accessTime;
        this.province = province;
        this.city = city;
        AdvId = advId;
    }
    public Log() {
        super();
        // TODO Auto-generated constructor stub
    }


    @Override
    public String toString() {
        return "Log{" +
                "userId='" + userId + '\'' +
                ", accessTime=" + accessTime +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", AdvId='" + AdvId + '\'' +
                '}';
    }
}
