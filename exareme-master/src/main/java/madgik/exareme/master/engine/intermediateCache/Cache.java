package madgik.exareme.master.engine.intermediateCache;

import madgik.exareme.common.schema.PhysicalTable;
import madgik.exareme.common.schema.Table;
import madgik.exareme.master.client.AdpDBClientProperties;
import madgik.exareme.master.engine.remoteQuery.impl.utility.Date;
import madgik.exareme.master.queryProcessor.decomposer.dag.NodeHashValues;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQueryParser;
import madgik.exareme.master.registry.Registry;
import madgik.exareme.utils.association.Triple;

import java.rmi.RemoteException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by christos on 12/4/2016.
 */
public class Cache {

    AdpDBClientProperties properties;
    private final static Lock lock = new ReentrantLock();
    private final long totalSize;
    private final List<String> removedTables;

    public Cache(AdpDBClientProperties properties) {

        this.properties = properties;
        this.totalSize = 0;
        removedTables = new LinkedList<>();
    }

    public Cache(AdpDBClientProperties properties, long totalSize) {
        this.properties = properties;
        this.totalSize = totalSize;
        removedTables = new LinkedList<>();
    }

    public void clearRemovedTables() {
        removedTables.clear();
    }


    private boolean validateDiskSpace(Map<String, Long> map1, Map<String, Long> map2) {

        for (String location : map2.keySet()) {

            Long size1 = map1.get(location);
            Long size2 = map2.get(location);
            System.out.println("1: " + size1);
            System.out.println("2: " + size2);
            System.out.println("3: " + totalSize);
            System.out.println("location " + location);
            if (size1 != null && (map1.get(location) + map2.get(location) > totalSize)) {
                System.out.println("Einai false to flag");
                return false;
            } else if (size1 == null && map2.get(location) > totalSize) {
                System.out.println("Einai false to flag");
                return false;
            }
        }
        System.out.println("Einai true to flag");
        return true;
    }

    public List<String> updateCache(Table newTable, Map<String, Long> sizeMap) throws ParseException {

        lock.lock();
        System.out.println("arxiiii");

        Registry registry = Registry.getInstance(properties.getDatabase());

        List<String> evictedTables = new LinkedList<String>();

        Map<String, Long> totalSizePerWorker;
        if (removedTables.isEmpty()) {
            totalSizePerWorker = registry.getWorkersSize();
        } else {
            totalSizePerWorker = registry.getWorkersSize(removedTables);
        }
        List<Table> tableInfos = registry.getTemporaryTablesCacheInfo();

        Map<String, Map<String, Long>> sizePerQuery = registry.getSizePerQuery();

        boolean computeBenefit = true;
        TreeMap<Double, List<Table>> tree = new TreeMap<>();
        List<Table> tableList;

        newTable.setBenefit(newTable.getNumOfAccess() / (Date.getDifferenceInSec(newTable.getLastAccess(), true) * newTable.getSize()));
        double benefit, newQueryBenefit = newTable.getBenefit();
        Table currentTable;

        while (!validateDiskSpace(totalSizePerWorker, sizeMap)) {


            if (computeBenefit) {
                for (Table table : tableInfos) {
                    if (table.getPin() == 0) {
                        table.setBenefit(table.getNumOfAccess() / (Date.getDifferenceInSec(table.getLastAccess(), true) * table.getSize()));
                        if (tree.containsKey(table.getBenefit())) {
                            tableList = tree.get(table.getBenefit());
                            tableList.add(table);
                        } else {
                            tableList = new LinkedList<>();
                            tableList.add(table);
                            tree.put(table.getBenefit(), tableList);
                        }
                    }
                }
                computeBenefit = false;
                System.out.println("start benefits");
                for (Double compBenefit : tree.keySet()) {
                    System.out.println("benefit " + compBenefit);
                    for (Table t : tree.get(compBenefit)) {
                        System.out.println("table " + t.getName());
                    }
                }
                System.out.println("end benefits");
            }

            if (tree.isEmpty()) {
                //delete to new table
                removedTables.add(newTable.getName());  //To actual delete toso se physical epipedo
                //oso kai sto registry ginete argotera!
                System.out.println("tpt dn einai unpinned epomenws dn tha mpei to " + newTable.getName());
                List<String> table = new ArrayList<>(1);
                table.add(newTable.getName());
                lock.unlock();
                return table;
            }

            benefit = tree.firstKey();
            System.out.println("founded benefit " + benefit);
            if (newQueryBenefit < benefit) {
                //delete to new table
                removedTables.add(newTable.getName());
                System.out.println("to benefit einai mikrotero gia to " + newTable.getName());

                List<String> table = new ArrayList<>(1);
                table.add(newTable.getName());
                lock.unlock();
                return table;
            }

            tableList = tree.get(benefit);
            System.out.println("prin gia benefit");
            for (Table t : tableList) {
                System.out.println("table " + t.getName());
            }
            currentTable = tableList.remove(0);
            System.out.println("meta");
            for (Table t : tableList) {
                System.out.println("table " + t.getName());
            }
            if (tableList.isEmpty()) {
                tree.remove(benefit);
            }
            evictedTables.add(currentTable.getName());
            removedTables.add(currentTable.getName());

            Long size;
            Map<String, Long> querySizePerLocation = sizePerQuery.get(currentTable.getName());
            System.out.println("start for");
            for (String location : querySizePerLocation.keySet()) {
                System.out.println("location : " + location + " with " + totalSizePerWorker.get(location));
                size = totalSizePerWorker.get(location);
                size -= querySizePerLocation.get(location);
                System.out.println("afairw " + querySizePerLocation.get(location));
                totalSizePerWorker.put(location, size);
                System.out.println("New location : " + location + " with " + totalSizePerWorker.get(location));
            }
            System.out.println("Start end");
        }

        for (String tableName : evictedTables) {
            //delete to table
            System.out.println("tha ginei delete to " + tableName);

        }
        System.out.println("mpike to " + newTable.getName());
        lock.unlock();
        totalSizePerWorker = null;
        tableInfos = null;
        sizePerQuery = null;
        tree = null;
        System.out.println("telosssss");

        return evictedTables;
    }

    public boolean pinTables(List<String> tables) {

        Registry registry = Registry.getInstance(properties.getDatabase());

        lock.lock();
        for (String table : tables) {
            if (!registry.containsPhysicalTable(table)) {
                lock.unlock();
                return false;
            }
        }

        for (String table : tables) {
            registry.pin(table);
        }

        lock.unlock();

        return true;
    }

    public void unpinTable(String table) {

        Registry registry = Registry.getInstance(properties.getDatabase());

        lock.lock();
        registry.unpin(table);
        lock.unlock();
    }

    public void updateCacheForTableUse(List<Table> tables) {

        Registry registry = Registry.getInstance(properties.getDatabase());

        lock.lock();
        registry.updateCacheForTableUse(tables);
        lock.unlock();
    }

    public String queryExistance(String query) {

        QueryContainment qc;
        SQLQuery sqlQuery = null;
        try {
            sqlQuery = SQLQueryParser.parse(query, new NodeHashValues());
            qc = new QueryContainment();
            qc.setDemandedQuery(sqlQuery);
        } catch (Exception e) {
            System.out.println("mpikaaa me to " + query);
            return null;
        }

        Registry registry = Registry.getInstance(properties.getDatabase());
        Collection<PhysicalTable> tables = registry.getPhysicalTables();
        for (PhysicalTable table : tables) {
            try {
                sqlQuery = SQLQueryParser.parse(table.getTable().getSqlQuery().replaceAll("_", " ").replaceAll("\\ {2,}", "_"), new NodeHashValues());
                qc.setCachedQuery(sqlQuery, table.getName());
                System.out.println("no error sto " + table.getTable().getSqlQuery().replaceAll("_", " ").replaceAll("\\ {2,}", "_"));
            } catch (Exception e) {
                System.out.println("error sto " + table.getTable().getSqlQuery().replaceAll("_", " ").replaceAll("\\ {2,}", "_"));
            }
        }

        return qc.containQuery();
    }

    //a)tableName, b)partitionColumn, c)numOfPartitions
    public Triple<String, String, Integer> queryHashIDExistance(int hashID) {

        Registry registry = Registry.getInstance(properties.getDatabase());
        return registry.containsHashIDInfo(hashID);
    }

    //a)tableName, b)partitionColumn, c)numOfPartitions
    public Triple<String, String, Integer> queryHashIDExistance(int hashID, long validDuration) {

        Registry registry = Registry.getInstance(properties.getDatabase());
        return registry.containsHashIDInfo(hashID, validDuration);
    }

    public String queryHashIDExistance(int hashID, String partitionColumn, int numOfPartitions) {

        Registry registry = Registry.getInstance(properties.getDatabase());

        String tablename = registry.containsHashID(hashID);
        if (tablename == null) {
            return null;
        } else {
            int numberOfPartitions = registry.getNumOfPartitions(tablename);
            if (numberOfPartitions != numOfPartitions) {
                return null;
            }
            String partColumn = registry.getPartitionColumn(tablename);
            if (partColumn != null && partitionColumn.equals(partColumn)) {
                return tablename;
            } else if (partColumn == null && partitionColumn == null) {
                return tablename;
            } else {
                return null;
            }
        }
    }

    public String queryHashIDExistance(int hashID, String partitionColumn, int numOfPartitions,
                                       long validDuration) throws ParseException {

        Registry registry = Registry.getInstance(properties.getDatabase());

        String tablename = registry.containsHashID(hashID, validDuration);
        if (tablename == null) {
            return null;
        } else {
            int numberOfPartitions = registry.getNumOfPartitions(tablename);
            if (numberOfPartitions != numOfPartitions) {
                return null;
            }
            String partColumn = registry.getPartitionColumn(tablename);
            if (partColumn != null && partitionColumn.equals(partColumn)) {
                return tablename;
            } else if (partColumn == null && partitionColumn == null) {
                return tablename;
            } else {
                return null;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        AdpDBClientProperties properties = new AdpDBClientProperties("/home/christos/database", "", "", false, false, -1, 10);
//        Cache cache = new Cache(properties);
//        System.out.println(cache.queryHashIDExistance(810901805));

        /* pin and unpin

//        ArrayList<String> tables = new ArrayList<>();
//        tables.add("mod_lessons");
//        tables.add("tmp_lessons");
////        tables.add("tmp_lesson");
////        boolean pinned = cache.pinTables(tables);
////        System.out.println("pinned= " + pinned);
//        cache.unpinTable("tmp_lessons" SQLQuery sqlQuery;
        try {
            sqlQuery = SQLQueryParser.parse("select_*_from_mod__lessons__v2".replaceAll("_", " "));
        } catch (Exception e) {
            sqlQuery = null;
            System.out.println("error sto " + "select_*_from_mod__lessons__v2".replaceAll("_", " ").replaceAll("\\ {2,}", "_"));
        });
//        cache.unpinTable("mod_lessons");


        */

        /*replacement

         */

//        periptwsi 1, tha fugei to mod_lessons
//        Table table = new Table("new_t");
//        table.setSize(65536);
//        table.setNumOfAccess(1);
//        table.setLastAccess("2016-01-15 12:16:49");
//        System.out.println(new java.util.Date().toString());
//        System.out.println(System.currentTimeMillis());

//        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
//        Date dateobj = new Date();
//        System.out.println(df.format(dateobj));
//        System.out.println(dateobj.toString());
//
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//        java.util.Date give_date = dateFormat.parse(new java.util.Date().toString());
//        System.out.println(give_date.toString());
//
//        table.setLastAccess(give_date.toString());

//        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
//        java.util.Date dateobj = new java.util.Date();
//        System.out.println("edw "+df.format(dateobj));
//        table.setLastAccess(df.format(dateobj));

//        Map<String, Integer> map = new HashMap<>();
//        map.put("192.168.1.3", 32768);
//
//        Cache cache = new Cache(properties, 327680);
//        cache.updateCache(table, map);

        Cache cache = new Cache(properties);
        String table = cache.queryHashIDExistance(0, null, 1, 10000);
        System.out.println("table is " + table);
        table = cache.queryHashIDExistance(256, null, 3, 10000);
        System.out.println("table is " + table);
//
//        System.out.println("apo edw ");
//        System.out.println(cache.queryExistance("select * from query_lessons1 where query_lessons1.idssss>5"));
//        System.out.println("edw 2");
//        System.out.println(cache.queryExistance("select * from l2 where l2.id>5"));

//        Triple<String, String, Integer> info = cache.queryHashIDExistance(56, 1000000);
        Triple<String, String, Integer> info = cache.queryHashIDExistance(56);
        if (info != null)
            System.out.println("info is " + info.getA() + " | " + info.getB() + " || " + info.getC());
        else
            System.out.println("info is null");

    }


}
