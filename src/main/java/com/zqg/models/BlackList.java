package com.zqg.models;


import org.apache.hadoop.hive.serde2.SerDeSpec;

import java.io.Serializable;
import java.util.Date;

public class BlackList implements Serializable {

   private   String    userId;
   private   String    advId;
   private   Date      time;
   private   Integer  clickTime;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAdvId() {
        return advId;
    }

    public void setAdvId(String advId) {
        this.advId = advId;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Integer getClickTime() {
        return clickTime;
    }

    public void setClickTime(Integer clickTime) {
        this.clickTime = clickTime;
    }


    public BlackList() {
    }

    public BlackList(String userId, String advId, Date time, Integer clickTime) {
        this.userId = userId;
        this.advId = advId;
        this.time = time;
        this.clickTime = clickTime;
    }

    @Override
    public String toString() {
        return "BlackList{" +
                "userId='" + userId + '\'' +
                ", advId='" + advId + '\'' +
                ", time=" + time +
                ", clickTime=" + clickTime +
                '}';
    }
}
