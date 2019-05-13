package com.cloudera.sa.cm;

import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.impala.analysis.*;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.tools.jstat.ParserException;

import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to parse SQL and record metrics.
 */
public class QueryBase {

    private static Logger LOGGER = LoggerFactory.getLogger(QueryBase.class);

    private HashSet<String> target;
    private HashSet<String> source;
    private HashSet<String> cteAlias;

    private String statement;
//    private double duration;
//    private double memory;
//    private double admissionWait;
    private TaskMetrics metrics;


    public QueryBase(String statement, TaskMetrics metrics) throws Exception {

        source = new HashSet<>();
        target = new HashSet<>();
        cteAlias = new HashSet<>();

//        this.duration = duration;
//        this.memory = memory;
        this.statement = statement;
        this.metrics = metrics;
//        this.admissionWait = admissionWait;

        parseImpala(statement.toLowerCase());
    }

    /**
     * Used to check SQL for Hive SQLs.
     * @param statement SQL String.
     * @return Validated SQL String.
     */
    public static String validate(String statement) {
        String createTableAsPattern = "(create\\s+table\\s+.*\\s+(stored\\s+as\\s+\\w+\\s+)?as\\s+)(\\()(.*)(\\))\\s*$";
        Pattern p = Pattern.compile(createTableAsPattern);
        Matcher m = p.matcher(statement.toLowerCase());

        if(m.find()) {
            if(LOGGER.isDebugEnabled()) {
                LOGGER.debug(m.group(1) + m.group(4));
            }
            return m.group(1) + m.group(4);
        } else {
            return statement;
        }
    }

    /**
     * Get source tables from SQL.
     * @return Source tables.
     */
    public Set<String> getSource() {
        return source;
    }

    /**
     * Get target tables from SQL.
     * @return Target tables.
     */
    public Set<String> getTarget() {
        return target;
    }

    /**
     * Use JSQL API to parse source/target tables.
     * @param statement SQL String.
     * @throws SemanticException
     * @throws ParserException
     * @throws JSQLParserException
     */
    @Deprecated
    private void parseJSQL(String statement) throws SemanticException, ParserException, JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse(statement);


        if (stmt instanceof Select) {
            Select selectStatement = (Select) stmt;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            source.addAll(tablesNamesFinder.getTableList(selectStatement));
        } else if (stmt instanceof CreateTable) {
            CreateTable createStatement = (CreateTable) stmt;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            source.addAll(tablesNamesFinder.getTableList(createStatement));
        } else if (stmt instanceof Insert) {
            Insert insert = (Insert) stmt;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            source.addAll(tablesNamesFinder.getTableList(insert));
        }
    }

    /**
     * Use Impala FE library to parse Impala SQL.
     * @param statement  SQL String.
     * @throws Exception
     */
    private void parseImpala(String statement) throws Exception {
        SqlScanner scanner = new SqlScanner(new StringReader(statement));
        SqlParser parser = new SqlParser(scanner);

        StatementBase stmt = (StatementBase) parser.parse().value;

        if (stmt instanceof CreateTableAsSelectStmt) {
            CreateTableAsSelectStmt createStmt = (CreateTableAsSelectStmt) stmt;
            target.add(createStmt.getInsertStmt().getTargetTableName().toString());
            parseImpalaQuery(createStmt.getQueryStmt());
        } else if (stmt instanceof CreateViewStmt) {
            CreateViewStmt createView = (CreateViewStmt) stmt;
            String targetTable = createView.getDb() + "." + createView.getTbl();
            target.add(targetTable);
            List<TableRef> tables = new ArrayList<>();
            createView.collectTableRefs(tables);
            for (TableRef table : tables) {
                getTableName(table);
            }
            source.remove(target);
        } else if (stmt instanceof InsertStmt) {
            InsertStmt insertStmt = (InsertStmt) stmt;
            String tableName = insertStmt.getTargetTableName().toString();
            target.add(tableName);

//            String withPattern = "(.*)insert\\s+(into|overwrite)\\s+" + tableName + ".*";
            // Extract String before insert.
            String withString = statement.replaceAll("insert\\s+(into|overwrite)\\s+" + tableName + "[\\d\\D]*", " ");
            String selectString = insertStmt.getQueryStmt().toSql();

            System.out.println(withString + selectString);
            SqlScanner queryScanner = new SqlScanner(new StringReader(withString + selectString));
            SqlParser queryParser = new SqlParser(queryScanner);
            QueryStmt queryStmt = (QueryStmt) queryParser.parse().value;
            parseImpalaQuery(queryStmt);
        } else {
            // We do not really care about rest
        }
    }

    /**
     * Get table name in the format as dbname.tablename.
     * @param table TableRef instance from Impala FE.
     * @return
     */
    private String getTableName(TableRef table) {
        return ToSqlUtils.getPathSql(table.getPath());
    }

    /**
     * Parse the query part of Impala SQL. Normally something after select / with expression.
     * @param statement QueryStmt instance from Impala FE.
     * @throws Exception
     */
    private void parseImpalaQuery(QueryStmt statement) throws Exception {
        List<TableRef> tables = new ArrayList<>();
        statement.collectTableRefs(tables);

        for (TableRef table : tables) {
            source.add(getTableName(table));
        }

        if (statement.hasWithClause()) {
            List<View> views = statement.getWithClause().getViews();
            for (View view : views) {
                source.remove(view.getFullName());
            }
        }
    }

    /**
     * Use Hive API to parse source/target tables.
     * @param statement SQL String.
     * @throws SemanticException
     * @throws ParseException
     */
    @Deprecated
    private void parseHive(String statement) throws SemanticException, ParseException {
        ParseDriver pd = new ParseDriver();
        ASTNode ast = pd.parse(statement);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Start to analyze sql: {}, ASTNode {}", statement, ast.dump());
        }

        while((ast.getToken() == null) && (ast.getChildCount() > 0)) {
            ast = (ASTNode) ast.getChild(0);
        }

        final NodeProcessor processor = new TableNameNodeProcessor();

        Map<Rule, NodeProcessor> rules = Maps.newLinkedHashMap();
        Dispatcher disp = new DefaultRuleDispatcher(processor, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        final List<Node> topNodes = Lists.newArrayList((Node) ast);
        ogw.startWalking(topNodes, null);
        source.remove(cteAlias);
    }

    /**
     * Used for Hive statement parse.
     */
    private class TableNameNodeProcessor implements  NodeProcessor {
        @Override
        public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
            ASTNode pt = (ASTNode) nd;
            switch (pt.getToken().getType()) {
                case HiveParser.TOK_TAB:
                    // Insert
                    String insertedTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                    target.add(insertedTable);
                    break;
                case HiveParser.TOK_TABREF:
                    // Table after from
                    String selectedTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                    source.add(selectedTable);
                    break;
                case HiveParser.TOK_CREATETABLE:
                    // Create
                    String createdTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                    target.add(createdTable);
                    break;
                case HiveParser.TOK_CTE:
                    // All CTE expression
                    ArrayList<Node> cteq = pt.getChildren();
                    for (Node subq : cteq) {
                        String ctealia = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ((ASTNode) subq).getChild(1));
                        cteAlias.add(ctealia);
                    }
                    break;
            }
            return null;
        }
    }

    /**
     * Get the SQL statement string.
     * @return SQL statement string.
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Get the metrics.
     * @return TaskMetrics
     */
    public TaskMetrics getMetrics() {
        return metrics;
    }

//    public double getDuration() {
//        return duration;
//    }
//
//    public double getMemory() {
//        return memory;
//    }
//
//    public void setDuration(double duration) {
//        this.duration = duration;
//    }
//
//    public void setMemory(double memory) {
//        this.memory = memory;
//    }
//
//    public double getAdmissionWait() {
//        return admissionWait;
//    }
//
//    public void setAdmissionWait(double admissionWait) {
//        this.admissionWait = admissionWait;
//    }
}
