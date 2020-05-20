package com.tbc.batchtask.bean;

//定义hbase中对应表的样式
public class ModulePvDailyTableInfo {
    private String family;
    private String date;
    private String module;
    private String pv;

    public ModulePvDailyTableInfo(String family, String date, String module, String pv) {
        this.family = family;
        this.date = date;
        this.module = module;
        this.pv = pv;
    }

    public ModulePvDailyTableInfo() {
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

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public String getPv() {
        return pv;
    }

    public void setPv(String pv) {
        this.pv = pv;
    }
}
