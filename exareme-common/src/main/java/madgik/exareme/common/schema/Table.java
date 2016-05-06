/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.common.schema;

import java.io.Serializable;

/**
 * @author herald
 */
public class Table implements Serializable {

    private static final long serialVersionUID = 1L;
    private String name = null;
    private String sqlDefinition = null;
    private String sqlQuery = null;
    private byte[] hashID;
    private boolean temp = false;
    private int totalSize = 0;
    private int pin = 0;
    private String last_access;
    private int numOfAccess;
    private String storageTime;
    private double benefit;

 // The level of a table view is the
    private int level = -1;

    public Table(String name) {
        this.name = name.toLowerCase();
    }

    public Table(String name, String sql) {
        this.name = name.toLowerCase();
        this.sqlDefinition = sql.trim();
    }

    public String getName() {
        return name;
    }

    public void setSize(int size) {
        totalSize = size;
    }

    public void setHashID(byte[] hashID) {
        this.hashID = hashID;
    }

    public void setSqlQuery(String query) {
        sqlQuery = query;
    }

    public void setLastAccess(String access) {
        last_access = access;
    }

    public void setStorageTime(String access) {
        storageTime = access;
    }

    public void setNumOfAccess(int access) {
        numOfAccess = access;
    }

    public void setBenefit(double benefit) {
        this.benefit = benefit;
    }

    public int getSize() {
        return totalSize;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public String getSqlDefinition() {
        return sqlDefinition;
    }

    public void setSqlDefinition(String sqlDefinition) {
        this.sqlDefinition = sqlDefinition.trim();
    }

    public boolean hasSQLDefinition() {
        return (sqlDefinition != null);
    }

    public boolean isTemp() {
        return temp;
    }

    public void setTemp(boolean temporary) {
        this.temp = temporary;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public byte[] getHashID() {
        return hashID;
    }

    public void setPin(int pin) {
        this.pin = pin;
    }

    public int getPin() {
        return pin;
    }

    public int getNumOfAccess() {
        return numOfAccess;
    }

    public String getLastAccess() {
        return last_access;
    }

    public String getStorageTime() {
        return storageTime;
    }

    public double getBenefit() {
        return benefit;
    }

    @Override
    public String toString() {
        return name + (temp ? " temp" : "");
    }
}