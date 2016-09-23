/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.master.client;

import madgik.exareme.common.app.engine.scheduler.elasticTree.client.SLA;

import java.io.Serializable;

/**
 * Read only client properties.
 *
 * @author alex
 */
public class AdpDBClientProperties implements Serializable {

	// client
	private String database;
	private String username;
	private String password;

	// engine hints
	private boolean tree;
	private boolean useHistory;
	private boolean validate;

	private int maxNumberOfContainers;
	private int statisticsUpdateSEC;

	private boolean cache = true;

	private SLA sla;

	public AdpDBClientProperties(String database, String username, String password, boolean tree, boolean useHistory,
			boolean validate, int maxNumberOfContainers, int statisticsUpdateSEC, SLA sla) {
		this.database = database;
		this.username = username;
		this.password = password;

		this.tree = tree;
		this.useHistory = useHistory;
		this.validate = validate;

		this.maxNumberOfContainers = maxNumberOfContainers;
		this.statisticsUpdateSEC = statisticsUpdateSEC;

		this.sla = sla;
	}

	public AdpDBClientProperties(String database, String username, String password, boolean cache, boolean tree,
			boolean useHistory, boolean validate, int maxNumberOfContainers, int statisticsUpdateSEC, SLA sla) {

		this.database = database;
		this.username = username;
		this.password = password;

        System.out.println("cache is "+cache);
        this.cache = cache;
		this.tree = tree;
		this.useHistory = useHistory;
		this.validate = validate;

		this.maxNumberOfContainers = maxNumberOfContainers;
		this.statisticsUpdateSEC = statisticsUpdateSEC;

		this.sla = sla;
	}

	public AdpDBClientProperties(String database) {
		this(database, "", "", false, false, true, -1, 10, null);
	}

	public AdpDBClientProperties(String database, String username, String password) {
		this(database, username, password, false, false, true, -1, 10, null);
	}

	public AdpDBClientProperties(String database, boolean cache) {
		this(database, "", "", cache, false, false, true, -1, 10, null);
	}

	public AdpDBClientProperties(String database, String username, String password, boolean cache, boolean useHistory,
			boolean validate, int maxNumberOfContainers, int statisticsUpdateSEC) {
		this(database, username, password, cache, false, useHistory, validate, maxNumberOfContainers,
				statisticsUpdateSEC, null);
	}

	public AdpDBClientProperties(String database, String username, String password, boolean useHistory,
			boolean validate, int maxNumberOfContainers, int statisticsUpdateSEC) {
		this(database, username, password, false, useHistory, validate, maxNumberOfContainers, statisticsUpdateSEC,
				null);
	}

	public String getDatabase() {
		return database;
	}

	public String getUsername() {
		return username;
	}

	public boolean isTreeEnabled() {
		return tree;
	}

	public boolean isHistoryUsed() {
		return useHistory;
	}

	public boolean isValidationEnabled() {
		return validate;
	}

	public int getMaxNumberOfContainers() {
		return maxNumberOfContainers;
	}

	public int getStatisticsUpdateSEC() {
		return statisticsUpdateSEC;
	}

	public SLA getSLA() {
		return this.sla;
	}

	public boolean isCachedEnable() {
		return cache;
	}

}
