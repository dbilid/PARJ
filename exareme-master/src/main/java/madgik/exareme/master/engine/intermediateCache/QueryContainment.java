package madgik.exareme.master.engine.intermediateCache;

import madgik.exareme.master.queryProcessor.decomposer.query.*;
import madgik.exareme.master.queryProcessor.decomposer.util.Pair;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by christos on 12/4/2016.
 */
public class QueryContainment {

    private List<Pair<String, SQLQuery>> cachedQuery;
    private SQLQuery demandedQuery;
    private Map<Column, List<NonUnaryWhereCondition>> filterOperandMapInDemandQuery;
    private Map<Column, List<NonUnaryWhereCondition>> joinOperandMapInDemandQuery;
    private List<Map<Column, List<NonUnaryWhereCondition>>> filterOperandMapInCachedQuery;
    private List<Map<Column, List<NonUnaryWhereCondition>>> joinOperandMapInCachedQuery;
    private Set<Table> demandedQueryInputTables;
    private List<Set<Table>> cachedQueryInputTables;
    private List<Set<String>> cachedSelectOutputs;
    private Set<NonUnaryWhereCondition> appliedDemandJoinConditions;
    private final String cachedTableName = "cachedTableName";
    private final String[] dateFormats = {
            "dd-mm-yyyy HH:mm:ss", "dd-mm-yyyy hh:mm:ss a", "dd/mm/yyyy HH:mm:ss",
            "dd/mm/yyyy hh:mm:ss a", "dd-MMM-yyyy HH:mm:ss", "dd-MMM-yyyy hh:mm:ss a",
            "dd/MMM/yyyy HH:mm:ss", "dd/MMM/yyyy hh:mm:ss a", "dd/mm/yyyy", "dd-mm-yyyy",
            "dd-MMM-yyyy", "dd/MMM/yyyy"};


    private void initCacheMaps(SQLQuery cachedQuery, String cacheTableName) {

        Set<Table> cacheTables = new HashSet<>();
        Set<String> cachedOutputs = new HashSet<>();

        Map<Column, List<NonUnaryWhereCondition>> filterMap = new HashMap<>(cachedQuery.getBinaryWhereConditions().size());
        Map<Column, List<NonUnaryWhereCondition>> joinMap = new HashMap<>(cachedQuery.getBinaryWhereConditions().size());

        List<NonUnaryWhereCondition> list;
        for (NonUnaryWhereCondition condition : cachedQuery.getBinaryWhereConditions()) {

            if (condition.getOperands().get(1).getClass().equals(Constant.class)) {
                if (filterMap.containsKey(condition.getOperands().get(0).getAllColumnRefs().get(0))) {
                    list = filterMap.get(condition.getOperands().get(0).getAllColumnRefs().get(0));
                    list.add(condition);
                } else {
                    list = new ArrayList<>();
                    list.add(condition);
                    filterMap.put(condition.getOperands().get(0).getAllColumnRefs().get(0), list);
                }
            } else {
                if (condition.getOperands().get(0).getAllColumnRefs().get(0).getAlias()
                        .compareTo(condition.getOperands().get(1).getAllColumnRefs().get(0).getAlias()) < 0
                        || (condition.getOperands().get(0).getAllColumnRefs().get(0).getAlias()
                        .equals(condition.getOperands().get(1).getAllColumnRefs().get(0).getAlias()) &&
                        condition.getOperands().get(0).getAllColumnRefs().get(0).getColumnName()
                                .compareTo(condition.getOperands().get(1).getAllColumnRefs().get(0).getColumnName()) < 0)) {

                    System.out.println("prinnnnnnnnnnnnnn " + condition);

                    Column tempColumn = new Column(condition.getOperands().get(0).getAllColumnRefs().get(0));
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setAlias(condition.getOperands().get(1).getAllColumnRefs().get(0).getAlias());
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setName(condition.getOperands().get(1).getAllColumnRefs().get(0).getName());
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setBaseTable(condition.getOperands().get(1).getAllColumnRefs().get(0).getBaseTable());
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setColumnName(condition.getOperands().get(1).getAllColumnRefs().get(0).getColumnName());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setAlias(tempColumn.getAlias());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setName(tempColumn.getName());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setBaseTable(tempColumn.getBaseTable());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setColumnName(tempColumn.getColumnName());
                    reverseOperation(condition);

                    System.out.println("meta " + condition);
                }
                if (joinMap.containsKey(condition.getOperands().get(0).getAllColumnRefs().get(0))) {
                    list = joinMap.get(condition.getOperands().get(0).getAllColumnRefs().get(0));
                    list.add(condition);
                } else {
                    list = new ArrayList<>();
                    list.add(condition);
                    joinMap.put(condition.getOperands().get(0).getAllColumnRefs().get(0), list);
                }
            }
        }

        for (Table table : cachedQuery.getInputTables()) {
            cacheTables.add(table);
        }

        if (cachedQuery.isSelectAll()) {
            cachedOutputs.add("*");
        } else {
            for (Output output : cachedQuery.getOutputs()) {
                cachedOutputs.add(output.getOutputName());
            }
        }

        if (filterOperandMapInCachedQuery == null) {
            filterOperandMapInCachedQuery = new LinkedList<>();
        }
        if (joinOperandMapInCachedQuery == null) {
            joinOperandMapInCachedQuery = new LinkedList<>();
        }
        if (cachedQueryInputTables == null) {
            cachedQueryInputTables = new LinkedList<>();
        }
        if (cachedSelectOutputs == null) {
            cachedSelectOutputs = new LinkedList<>();
        }

        cachedQueryInputTables.add(cacheTables);
        cachedSelectOutputs.add(cachedOutputs);
        filterOperandMapInCachedQuery.add(filterMap);
        joinOperandMapInCachedQuery.add(joinMap);
        this.cachedQuery.add(new Pair(cacheTableName, cachedQuery));

    }

    private void initDemandMaps() {

        filterOperandMapInDemandQuery = new HashMap<>(demandedQuery.getBinaryWhereConditions().size());
        joinOperandMapInDemandQuery = new HashMap<>(demandedQuery.getBinaryWhereConditions().size());

        List<NonUnaryWhereCondition> list;
        for (NonUnaryWhereCondition condition : demandedQuery.getBinaryWhereConditions()) {

            if (condition.getOperands().get(1).getClass().equals(Constant.class)) {
                if (filterOperandMapInDemandQuery.containsKey(condition.getOperands().get(0).getAllColumnRefs().get(0))) {
                    list = filterOperandMapInDemandQuery.get(condition.getOperands().get(0).getAllColumnRefs().get(0));
                    list.add(condition);
                } else {
                    list = new ArrayList<>();
                    list.add(condition);
                    filterOperandMapInDemandQuery.put(condition.getOperands().get(0).getAllColumnRefs().get(0), list);
                }
            } else {

                if (condition.getOperands().get(0).getAllColumnRefs().get(0).getAlias()
                        .compareTo(condition.getOperands().get(1).getAllColumnRefs().get(0).getAlias()) < 0
                        || (condition.getOperands().get(0).getAllColumnRefs().get(0).getAlias()
                        .equals(condition.getOperands().get(1).getAllColumnRefs().get(0).getAlias()) &&
                        condition.getOperands().get(0).getAllColumnRefs().get(0).getColumnName()
                                .compareTo(condition.getOperands().get(1).getAllColumnRefs().get(0).getColumnName()) < 0)) {

                    System.out.println("prinnnnnnnnnnnnnn " + condition);

                    Column tempColumn = new Column(condition.getOperands().get(0).getAllColumnRefs().get(0));
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setAlias(condition.getOperands().get(1).getAllColumnRefs().get(0).getAlias());
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setName(condition.getOperands().get(1).getAllColumnRefs().get(0).getName());
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setBaseTable(condition.getOperands().get(1).getAllColumnRefs().get(0).getBaseTable());
                    condition.getOperands().get(0).getAllColumnRefs().get(0)
                            .setColumnName(condition.getOperands().get(1).getAllColumnRefs().get(0).getColumnName());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setAlias(tempColumn.getAlias());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setName(tempColumn.getName());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setBaseTable(tempColumn.getBaseTable());
                    condition.getOperands().get(1).getAllColumnRefs().get(0)
                            .setColumnName(tempColumn.getColumnName());
                    reverseOperation(condition);

                    System.out.println("meta " + condition);
                }

                if (joinOperandMapInDemandQuery.containsKey(condition.getOperands().get(0).getAllColumnRefs().get(0))) {
                    list = joinOperandMapInDemandQuery.get(condition.getOperands().get(0).getAllColumnRefs().get(0));
                    list.add(condition);
                } else {
                    list = new ArrayList<>();
                    list.add(condition);
                    joinOperandMapInDemandQuery.put(condition.getOperands().get(0).getAllColumnRefs().get(0), list);
                }
            }
        }

        demandedQueryInputTables = new HashSet<>();
        for (Table table : demandedQuery.getInputTables()) {
            demandedQueryInputTables.add(table);
        }

    }

    public QueryContainment(SQLQuery cachedQuery, String cacheTableName, SQLQuery demandedQuery) {
        this.cachedQuery = new LinkedList<>();
        this.demandedQuery = demandedQuery;
        initDemandMaps();
        initCacheMaps(cachedQuery, cacheTableName);
    }

    public QueryContainment(SQLQuery demandedQuery) {
        this.demandedQuery = demandedQuery;
        initDemandMaps();
    }

    public QueryContainment() {

    }

    public void setQueries(SQLQuery cachedQuery, String cacheTableName, SQLQuery demandedQuery) {
        this.cachedQuery = new LinkedList<>();
        this.demandedQuery = demandedQuery;
        initDemandMaps();
        initCacheMaps(cachedQuery, cacheTableName);
    }

    public void setCachedQuery(SQLQuery cachedQuery, String cacheTableName) {
        if (this.cachedQuery == null) {
            this.cachedQuery = new LinkedList<>();
        }
        initCacheMaps(cachedQuery, cacheTableName);
    }

    public void setDemandedQuery(SQLQuery demandedQuery) {
        this.demandedQuery = demandedQuery;
        initDemandMaps();
    }

    private NonUnaryWhereCondition joinCanBeApplied(String cacheOperator, String demandOperator,
                                                    Operand cacheQueryOperand, Operand demandQueryOperand) {

        NonUnaryWhereCondition condition;
        System.out.println("cache:" + cacheOperator + "|" + cacheQueryOperand);
        System.out.println("demand:" + demandOperator + "|" + demandQueryOperand);

        if (cacheOperator.equals(">=")) {
            if (demandOperator.equals(">=") && cacheQueryOperand.equals(demandQueryOperand)) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return condition;
            } else if (demandOperator.equals(">") && cacheQueryOperand.equals(demandQueryOperand)) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(demandQueryOperand);
                return condition;
            } else {
                return null;
            }
        } else if (cacheOperator.equals("<=")) {
            if (demandOperator.equals("<=") && cacheQueryOperand.equals(demandQueryOperand)) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return condition;
            } else if (demandOperator.equals("<") && cacheQueryOperand.equals(demandQueryOperand)) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(demandQueryOperand);
                return condition;
            } else {
                return null;
            }
        } else {
            if (cacheOperator.equals(demandOperator) && cacheQueryOperand.equals(demandQueryOperand)) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return condition;
            } else {
                return null;
            }
        }
    }

    private boolean checkEqualityFilterApplication(NonUnaryWhereCondition demandCondition, int cachedQueryIndex) {

        String demandQueryOperator = demandCondition.getOperator();
        if (demandQueryOperator.equals("=")) {

            Constant demandConstant = (Constant) demandCondition.getOperands().get(1);
            double demandValue = Double.parseDouble(demandConstant.getValue().toString());
            Column demandFilterColumn = demandCondition.getOperands().get(0).getAllColumnRefs().get(0);

            Map<Column, List<NonUnaryWhereCondition>> cacheFilterMap
                    = filterOperandMapInCachedQuery.get(cachedQueryIndex);

            Double maxDownLimit = null, minDownLimit = null, maxUpLimit = null, minUpLimit = null;
            if (cacheFilterMap.containsKey(demandFilterColumn)) {
                for (NonUnaryWhereCondition cacheCondition : cacheFilterMap.get(demandFilterColumn)) {

                    Constant cacheConstant = (Constant) cacheCondition.getOperands().get(1);
                    double cacheValue = Double.parseDouble(cacheConstant.getValue().toString());
                    String cachedQueryOperator = cacheCondition.getOperator();

                    switch (cachedQueryOperator) {
                        case "<=":

                            if (minUpLimit == null) {
                                minUpLimit = cacheValue;
                                maxUpLimit = cacheValue;
                            }

                            if (cacheValue < minUpLimit) {
                                minUpLimit = cacheValue;
                            } else if (cacheValue > maxUpLimit) {
                                maxUpLimit = cacheValue;
                            }
                            break;
                        case "<":

                            if (minUpLimit == null) {
                                minUpLimit = cacheValue;
                                maxUpLimit = cacheValue;
                            }

                            if ((cacheValue - Double.MIN_VALUE) < minUpLimit) {
                                minUpLimit = cacheValue - Double.MIN_VALUE;
                            } else if ((cacheValue - Double.MIN_VALUE) > maxUpLimit) {
                                maxUpLimit = cacheValue - Double.MIN_VALUE;
                            }
                            break;
                        case ">=":

                            if (minDownLimit == null) {
                                minDownLimit = cacheValue;
                                maxDownLimit = cacheValue;
                            }

                            if (cacheValue < minDownLimit) {
                                minDownLimit = cacheValue;
                            } else if (cacheValue > maxDownLimit) {
                                maxDownLimit = cacheValue;
                            }
                            break;
                        case ">":

                            if (minDownLimit == null) {
                                minDownLimit = cacheValue;
                                maxDownLimit = cacheValue;
                            }

                            if ((cacheValue - Double.MIN_VALUE) < minDownLimit) {
                                minDownLimit = cacheValue - Double.MIN_VALUE;
                            } else if ((cacheValue - Double.MIN_VALUE) > maxDownLimit) {
                                maxDownLimit = cacheValue - Double.MIN_VALUE;
                            }
                            break;
                        default:
                            break;

                    }
                }
            }


            if (maxDownLimit == null && maxUpLimit == null) { //dn uparxoun aniswtites
                return false;
            } else if (maxDownLimit == null) {  //uparxoun mono katw anisothtes(x<, x<=)
                return (demandValue <= minUpLimit);
            } else if (maxUpLimit == null) {    //uparxoun mono anw anusothtes(x>, x>=)
                return (demandValue >= maxDownLimit);
            } else if (minUpLimit < maxDownLimit) {    //keno sunolo
                return false;
            } else if (demandValue >= maxDownLimit && demandValue <= minUpLimit) {
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    private boolean isDouble(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isValidDate(String dateString, String dateFormat) {
        SimpleDateFormat df = new SimpleDateFormat(dateFormat);
        try {
            df.parse(dateString);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    private String isDate(String dateString) {

        boolean result;
        for (String dateFormat : dateFormats) {
            result = isValidDate(dateString.substring(1, dateString.length()-1), dateFormat);
            if (result) {
                return dateFormat;
            }
        }
        return null;
    }

    private Pair<NonUnaryWhereCondition, Double> filterCanBeApplied(String cacheOperator, String demandOperator,
                                                                    Constant cacheConstant, Constant demandConstant) {

        System.out.println("cache:" + cacheOperator + "|" + cacheConstant);
        System.out.println("demand:" + demandOperator + "|" + demandConstant);

        NonUnaryWhereCondition condition;
        double numericCacheValue, numericDemandValue;
        double diff;
        String cacheValue = cacheConstant.getValue().toString();
        String demandValue = demandConstant.getValue().toString();
        if (isDouble(cacheConstant.getValue().toString()) && isDouble(demandConstant.getValue().toString())) {
            numericCacheValue = Double.parseDouble(cacheConstant.getValue().toString());
            numericDemandValue = Double.parseDouble(demandConstant.getValue().toString());
            diff = numericDemandValue - numericCacheValue;
        } else {
            String dateFormat1, dateFormat2;
            dateFormat1 = isDate(cacheValue);
            dateFormat2 = isDate(demandValue);
            if (dateFormat1 != null && dateFormat2 != null && dateFormat1.equals(dateFormat2)) {
                SimpleDateFormat format = new SimpleDateFormat(dateFormat1);
                try {
                    diff = format.parse(demandValue.substring(1, demandValue.length() - 1)).getTime()
                            - format.parse(cacheValue.substring(1, cacheValue.length()-1)).getTime();
                } catch (ParseException e) {
                    return null;
                }
            } else {
                diff = demandValue.compareTo(cacheValue);
            }
        }

        if (cacheOperator.equals("=")) {
            if (demandOperator.equals("=") && diff == 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return new Pair(condition, 0.0);
            } else {
                return null;
            }
        } else if (cacheOperator.equals(">")) {
            if (demandOperator.equals(">") && diff == 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return new Pair(condition, 0.0);
            } else if (demandOperator.equals(">") && diff > 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(new Constant(demandValue));
                return new Pair(condition, diff);
            } else if (demandOperator.equals(">=") && diff > 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(new Constant(demandValue));
                return new Pair(condition, diff);
            } else if (demandOperator.equals("=") && diff > 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("partialTransformation");
                return new Pair(condition, -1.0);
            } else {
                return null;
            }
        } else if (cacheOperator.equals("<")) {
            if (demandOperator.equals("<") && diff == 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return new Pair(condition, 0.0);
            } else if (demandOperator.equals("<") && diff < 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(new Constant(demandValue));
                return new Pair(condition, -diff);
            } else if (demandOperator.equals("<=") && diff < 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(new Constant(demandValue));
                return new Pair(condition, -diff);
            } else if (demandOperator.equals("=") && diff < 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("partialTransformation");
                return new Pair(condition, -1.0);
            } else {
                return null;
            }
        } else if (cacheOperator.equals(">=")) {
            if (demandOperator.equals(">=") && diff == 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return new Pair(condition, 0.0);
            } else if ((demandOperator.equals(">") || demandOperator.equals(">=")) && diff >= 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(new Constant(demandValue));
                return new Pair(condition, diff);
            } else if (demandOperator.equals("=") && diff >= 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("partialTransformation");
                return new Pair(condition, diff);
            } else {
                return null;
            }
        } else if (cacheOperator.equals("<=")) {
            if (demandOperator.equals("<=") && diff == 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return new Pair(condition, 0.0);
            } else if ((demandOperator.equals("<") || demandOperator.equals("<=")) && diff <= 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator(demandOperator);
                condition.setRightOp(new Constant(demandValue));
                return new Pair(condition, -diff);
            } else if (demandOperator.equals("=") && diff <= 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("partialTransformation");
                return new Pair(condition, -1.0);
            } else {
                return null;
            }
        } else if (cacheOperator.equals("!=")) {
            if (diff == 0) {
                condition = new NonUnaryWhereCondition();
                condition.setOperator("");
                return new Pair(condition, 0.0);
            } else {
                return null;
            }
        } else {  //in, exist, any, ...
            return null;
        }
    }

    private void reverseOperation(NonUnaryWhereCondition condition) {

        switch (condition.getOperator()) {

            case "=":
                break;
            case "!=":
                break;
            case ">":
                condition.setOperator("<");
                break;
            case ">=":
                condition.setOperator("<=");
                break;
            case "<":
                condition.setOperator(">");
                break;
            case "<=":
                condition.setOperator(">=");
                break;
        }
    }

    private String contain(int queryIndex) {

        NonUnaryWhereCondition appliedTransformation;
        Operand cachedQueryOperand, demandQueryOperand;
        Constant cacheConstant, demandConstant;
        String cachedQueryOperator, demandQueryOperator;
        NonUnaryWhereCondition condition = null;
        SQLQuery sqlQuery = new SQLQuery();
        appliedDemandJoinConditions = new HashSet<>();

        Map<Column, List<NonUnaryWhereCondition>> cacheJoinMap = joinOperandMapInCachedQuery.get(queryIndex);
        Map<Column, List<NonUnaryWhereCondition>> cacheFilterMap = filterOperandMapInCachedQuery.get(queryIndex);
        Set<Table> cachedTables = cachedQueryInputTables.get(queryIndex);
        String cacheTable = cachedQuery.get(queryIndex).getVar1();

        Set<Table> usedTables = new HashSet<>();
        for (Table table : cachedTables) {
            if (!demandedQueryInputTables.contains(table)) {
                System.out.println("table " + table.toString() + " is not exist in demand query");
                return null;
            }
        }

        String tableName, alias, columnName;
        for (Column cachedFilterColumn : cacheFilterMap.keySet()) {

            if (!filterOperandMapInDemandQuery.containsKey(cachedFilterColumn)) {
                System.out.println("demand query does not has the filter column " + cachedFilterColumn);
                return null;
            }

            for (NonUnaryWhereCondition cacheCondition : cacheFilterMap.get(cachedFilterColumn)) {

                cacheConstant = (Constant) cacheCondition.getOperands().get(1);
                cachedQueryOperator = cacheCondition.getOperator();

                System.out.println("@@@validate " + cachedFilterColumn.toString() + " & " + cachedQueryOperator + " & " + cacheConstant);

                NonUnaryWhereCondition selectedCondition = null;
                boolean transformationCanBeApplied = false;
                boolean removal = true;
                double leastDistance = -2.0, distance = 0.0;

                for (NonUnaryWhereCondition demandCondition : filterOperandMapInDemandQuery.get(cachedFilterColumn)) {

                    demandConstant = (Constant) demandCondition.getOperands().get(1);
                    demandQueryOperator = demandCondition.getOperator();

                    Pair<NonUnaryWhereCondition, Double> result = filterCanBeApplied(cachedQueryOperator, demandQueryOperator, cacheConstant, demandConstant);
                    if (result == null) {
                        appliedTransformation = null;
                    } else {
                        appliedTransformation = result.getVar1();
                        distance = result.getVar2();
                    }
                    if (appliedTransformation != null) {
                        condition = demandCondition;

                        if (appliedTransformation.getOperator().isEmpty()) {
                            transformationCanBeApplied = true;
                            removal = true;
                            leastDistance = 0.0;
                            break;
                        } else if (appliedTransformation.getOperator().equals("partialTransformation")) {
                            if (leastDistance < 0) {
                                transformationCanBeApplied = true;
                                removal = false;
                            }
                        } else {

                            if (leastDistance < 0 || leastDistance > distance) {
//                                tableName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getBaseTable();
                                alias = demandCondition.getLeftOp().getAllColumnRefs().get(0).getAlias();
                                tableName = getBaseTableName(alias, demandedQuery);
                                columnName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getColumnName();

                                usedTables.add(new Table(tableName, alias));
                                Column column = new Column(cachedTableName, alias + "_" + columnName, cacheTable);

                                appliedTransformation.setLeftOp(column);
                                if (appliedTransformation == null) {
                                    System.out.println("is null");
                                }
                                selectedCondition = appliedTransformation;
                                transformationCanBeApplied = true;
                                removal = true;
                                leastDistance = distance;
                            }
                        }
                    }
                }

                if (!transformationCanBeApplied) { //dn uparxei eite o operator, eite dn mporei na efarmosthei
                    return null;
                }
                if (removal) {
                    filterOperandMapInDemandQuery.get(cachedFilterColumn).remove(condition);
                    if (leastDistance != 0.0) {
                        sqlQuery.addBinaryWhereCondition(selectedCondition);
                        System.out.println("tha kanw apply to " + selectedCondition.toString());
                    }
                }
            }
        }

        for (Column cachedJoinColumn : cacheJoinMap.keySet()) {

            if (!joinOperandMapInDemandQuery.containsKey(cachedJoinColumn)) {
                System.out.println("demand query does not has the join column " + cachedJoinColumn);
                return null;
            }

            for (NonUnaryWhereCondition cacheCondition : cacheJoinMap.get(cachedJoinColumn)) {

                cachedQueryOperand = cacheCondition.getOperands().get(1);
                cachedQueryOperator = cacheCondition.getOperator();

                System.out.println("@@@validate " + cachedJoinColumn.toString() + " & " + cachedQueryOperator + " & " + cachedQueryOperand.toString());

                boolean transformationCanBeApplied = false;

                for (NonUnaryWhereCondition demandCondition : joinOperandMapInDemandQuery.get(cachedJoinColumn)) {

                    demandQueryOperand = demandCondition.getOperands().get(1);
                    demandQueryOperator = demandCondition.getOperator();

                    appliedTransformation = joinCanBeApplied(cachedQueryOperator, demandQueryOperator, cachedQueryOperand, demandQueryOperand);
                    if (appliedTransformation != null) {

                        if (appliedTransformation.getOperator().isEmpty()) {
                            transformationCanBeApplied = true;
                            appliedDemandJoinConditions.add(demandCondition);
                            break;
                        } else {

//                            tableName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getBaseTable();
                            alias = demandCondition.getLeftOp().getAllColumnRefs().get(0).getAlias();
                            tableName = getBaseTableName(alias, demandedQuery);
                            columnName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getColumnName();

                            Column column = new Column(cachedTableName, alias + "_" + columnName, cacheTable);
                            appliedTransformation.setLeftOp(column);
                            usedTables.add(new Table(tableName, alias));

//                            tableName = demandCondition.getRightOp().getAllColumnRefs().get(0).getBaseTable();
                            alias = demandCondition.getRightOp().getAllColumnRefs().get(0).getAlias();
                            tableName = getBaseTableName(alias, demandedQuery);
                            columnName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getColumnName();

                            column = new Column(cachedTableName, alias + "_" + columnName, cacheTable);
                            appliedTransformation.setRightOp(column);

                            usedTables.add(new Table(tableName, alias));

                            appliedTransformation.setLeftOp(column);
                            sqlQuery.addBinaryWhereCondition(appliedTransformation);
                            appliedDemandJoinConditions.add(appliedTransformation);
                            System.out.println("tha kanw apply to " + appliedTransformation.toString());
                            transformationCanBeApplied = true;
                            break;
                        }
                    }

                }

                if (!transformationCanBeApplied) { //dn uparxei eite o operator, eite dn mporei na efarmosthei
                    return null;
                }

            }
        }

        Set<Table> addedTables = new HashSet<>();
        Table table;
        for (Column demandFilterColumn : filterOperandMapInDemandQuery.keySet()) {
            for (NonUnaryWhereCondition demandCondition : filterOperandMapInDemandQuery.get(demandFilterColumn)) {

//                tableName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getBaseTable();
                alias = demandCondition.getLeftOp().getAllColumnRefs().get(0).getAlias();
                tableName = getBaseTableName(alias, demandedQuery);
                columnName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getColumnName();

                table = new Table(tableName, alias);

                System.out.println("alias " + table.getAlias() + " kai tableName " + table.getName());
                if (!cachedQueryInputTables.get(queryIndex).contains(table)) {
                    if (!addedTables.contains(table) && !usedTables.contains(table)) {
                        addedTables.add(table);
                    }
                    sqlQuery.addBinaryWhereCondition(demandCondition);
                    System.out.println("add demand filter condition: " + demandCondition.toString());
                } else {    //prokeitai gia filtro sto cached table

                    if (!cachedSelectOutputs.get(queryIndex).contains("*")
                            && !cachedSelectOutputs.get(queryIndex).contains(alias + "_" + columnName)) {
                        System.out.println("to where filtro pedio " + alias + "_" + columnName + " dn uparxei sto cached query");
                        return null;
                    }

                    Column leftOp = new Column(cachedTableName, alias + "_" + columnName, cacheTable);
                    NonUnaryWhereCondition newCondition = new NonUnaryWhereCondition(leftOp, demandCondition.getRightOp(),
                            demandCondition.getOperator());
                    sqlQuery.addBinaryWhereCondition(newCondition);
                    System.out.println("add demand filter condition: " + newCondition);
                }

//                if (!checkEqualityFilterApplication(demandCondition, queryIndex)) {
//                Column leftOp = new Column("cachedTable", alias+"_"+columnName, "baseTableNaMpei");
//                NonUnaryWhereCondition newCondition = new NonUnaryWhereCondition(leftOp, demandCondition.getRightOp(),
//                            demandCondition.getOperator());
//                sqlQuery.addBinaryWhereCondition(newCondition);
//                System.out.println("add demand filter condition: " + newCondition);

//                    sqlQuery.addBinaryWhereCondition(demandCondition);
//                    System.out.println("add demand filter condition: " + demandCondition.toString());
//                }
            }
        }

        for (Column demandJoinColumn : joinOperandMapInDemandQuery.keySet()) {
            for (NonUnaryWhereCondition demandCondition : joinOperandMapInDemandQuery.get(demandJoinColumn)) {

                if (!appliedDemandJoinConditions.contains(demandCondition)) {
//                    tableName = demandCondition.getRightOp().getAllColumnRefs().get(0).getBaseTable();
                    alias = demandCondition.getRightOp().getAllColumnRefs().get(0).getAlias();
                    tableName = getBaseTableName(alias, demandedQuery);
                    columnName = demandCondition.getRightOp().getAllColumnRefs().get(0).getColumnName();
                    Column rightOp, leftOp;
                    table = new Table(tableName, alias);

                    if (!cachedQueryInputTables.get(queryIndex).contains(table)) {
                        if (!addedTables.contains(table) && !usedTables.contains(table)) {
                            addedTables.add(table);
                        }
                        rightOp = (Column) demandCondition.getRightOp();
                    } else {    //to table einai cached
                        if (!cachedSelectOutputs.get(queryIndex).contains("*")
                                && !cachedSelectOutputs.get(queryIndex).contains(alias + "_" + columnName)) {
                            System.out.println("to join pedio " + alias + "_" + columnName + " dn uparxei sto cached query");
                            return null;
                        }
                        rightOp = new Column(cachedTableName, alias + "_" + columnName, cacheTable);
                    }

//                    tableName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getBaseTable();
                    alias = demandCondition.getLeftOp().getAllColumnRefs().get(0).getAlias();
                    tableName = getBaseTableName(alias, demandedQuery);
                    columnName = demandCondition.getLeftOp().getAllColumnRefs().get(0).getColumnName();
                    table = new Table(tableName, alias);

                    if (!cachedQueryInputTables.get(queryIndex).contains(table)) {
                        if (!addedTables.contains(table) && !usedTables.contains(table)) {
                            addedTables.add(table);
                        }
                        leftOp = (Column) demandCondition.getLeftOp();
                    } else {    //to table einai cached
                        if (!cachedSelectOutputs.get(queryIndex).contains("*")
                                && !cachedSelectOutputs.get(queryIndex).contains(alias + "_" + columnName)) {
                            System.out.println("to join pedio " + alias + "_" + columnName + " dn uparxei sto cached query");
                            return null;
                        }
                        leftOp = new Column(cachedTableName, alias + "_" + columnName, cacheTable);
                    }

                    NonUnaryWhereCondition newCondition = new NonUnaryWhereCondition(leftOp, rightOp, demandCondition.getOperator());
                    sqlQuery.addBinaryWhereCondition(newCondition);
                    System.out.println("add demand join condition: " + newCondition.toString());
                }
            }
        }

        Table cachedTable = new Table(cacheTable, cachedTableName);
        sqlQuery.addInputTable(cachedTable);
        for (Table addedTable : addedTables) {
            sqlQuery.addInputTable(addedTable);
            System.out.println("add demand table " + addedTable);
        }


        if (demandedQuery.isSelectAll()) {
            sqlQuery.setSelectAll(true);
        } else {
            String[] data;
            for (Output output : demandedQuery.getOutputs()) {
                data = output.getObject().getAllColumnRefs().get(0).toString().split("\\.");

                Table selectTable = new Table(null, data[0]);
                if (!cachedQueryInputTables.get(queryIndex).contains(selectTable)) {
                    sqlQuery.addOutput(data[0], data[1], output.getOutputName());
                } else {
                    if (cachedSelectOutputs.get(queryIndex).contains(data[0] + "_" + data[1]) ||
                            cachedSelectOutputs.get(queryIndex).contains("*")) {
                        sqlQuery.addOutput(cachedTableName, data[0] + "_" + data[1], output.getOutputName());
                    } else {
                        System.out.println("to select pedio " + data[0] + "_" + data[1] + " dn uparxei sto cached query");
                        return null;
                    }
                }

            }
        }
        return sqlQuery.toSQL();
    }

    public String containQuery() {

        String result = null;

        for (int i = 0; i < cachedQuery.size(); ++i) {
            result = contain(i);

            if (result != null) {
                return result;
            }
        }

        return null;
    }

    public void clear() {

        cachedQuery.clear();
        cachedSelectOutputs.clear();
        demandedQuery = null;
        filterOperandMapInDemandQuery.clear();
        joinOperandMapInDemandQuery.clear();
        filterOperandMapInCachedQuery.clear();
        joinOperandMapInCachedQuery.clear();
        demandedQueryInputTables.clear();
        cachedQueryInputTables.clear();
        appliedDemandJoinConditions.clear();
    }

    private String getBaseTableName(String tableAlias, SQLQuery sqlQuery){

        for(Table table : sqlQuery.getInputTables()){
            if(table.getAlias().equals(tableAlias)){
                return table.getlocalName();
            }
        }
        return null;
    }
}
