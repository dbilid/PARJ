/**
 * Copyright MaDgIK Group 2010 - 2012.
 */
package madgik.exareme.utils.embedded.db;

import java.io.Serializable;

/**
 * @author herald
 */
public class TableInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tableName = null;
    private String sqlDefinition = null;
    private String sqlQuery;
    private Long sizeInBytes = null;
    private String location = null;

    public TableInfo(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setSqlDefinition(String sqlDefinition) {
        this.sqlDefinition = sqlDefinition;
    }

    public void setSizeInBytes(Long size) {
        this.sizeInBytes = size;
    }

    public void setSqlQuery(String query) {
        this.sqlQuery = query;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getSQLDefinition() {
        return sqlDefinition;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public String getSqlQuery() {
        return this.sqlQuery;
    }

    public String getLocation() {
        return location;
    }
}
