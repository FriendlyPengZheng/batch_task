package com.tbc.batchtask.bean;

//统计昨天的pv
public class PvDailyTableInfo {
    private String family;
    private String date;
    private String pv;

    public PvDailyTableInfo(String family, String date, String pv) {
        this.family = family;
        this.date = date;
        this.pv = pv;
    }

    public PvDailyTableInfo() {
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPv() {
        return pv;
    }

    public void setPv(String pv) {
        this.pv = pv;
    }
}
