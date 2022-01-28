package com.aofeng.label.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import scala.Serializable;

@ToString
@Getter
@Setter
public class MetaStoreConfig implements Serializable {
    String driver;
    String url;
    String user;
    String password;
    String tbl_report;
    String tbl_schedules;
    String initialSize;
    String maxActive;
    String minIdle;
    String maxWait;

    String tbl_actions;
    String atid;
    String bstatus;
    String create_time;
    String tbl_batch_user;
    String userids;
    String monthAgo;

    public String getMonthAgo() {
        return monthAgo;
    }

    public void setMonthAgo(String monthAgo) {
        this.monthAgo = monthAgo;
    }


    public String getTbl_actions() {
        return tbl_actions;
    }

    public void setTbl_actions(String tbl_actions) {
        this.tbl_actions = tbl_actions;
    }

    public String getAtid() {
        return atid;
    }

    public void setAtid(String atid) {
        this.atid = atid;
    }

    public String getBstatus() {
        return bstatus;
    }

    public void setBstatus(String bstatus) {
        this.bstatus = bstatus;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getTbl_batch_user() {
        return tbl_batch_user;
    }

    public void setTbl_batch_user(String tbl_batch_user) {
        this.tbl_batch_user = tbl_batch_user;
    }

    public String getUserids() {
        return userids;
    }

    public void setUserids(String userids) {
        this.userids = userids;
    }


    public String getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(String initialSize) {
        this.initialSize = initialSize;
    }

    public String getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(String maxActive) {
        this.maxActive = maxActive;
    }

    public String getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(String minIdle) {
        this.minIdle = minIdle;
    }

    public String getMaxWait() {
        return maxWait;
    }

    public void setMaxWait(String maxWait) {
        this.maxWait = maxWait;
    }


    public String getTbl_report() {
        return tbl_report;
    }

    public void setTbl_report(String tbl_report) {
        this.tbl_report = tbl_report;
    }

    public String getTbl_schedules() {
        return tbl_schedules;
    }

    public void setTbl_schedules(String tbl_schedules) {
        this.tbl_schedules = tbl_schedules;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
