package my.test.db.compaction;

import my.test.TestBase;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

public class LeveledCompactionTest extends TestBase {
    public static void main(String[] args) throws Exception {
        new LeveledCompactionTest().start();
    }

    @Override
    public void startInternal() throws Exception {
        tableName = "LeveledCompactionTest";
        //create();
        insert();
        select();
    }

    void create() throws Exception {
        cql = "CREATE TABLE IF NOT EXISTS " + tableName + //
                " (id int, f1 int, f2 text, f3 int, PRIMARY KEY (id, f1))" + //
                "WITH compaction = { 'class' : 'LeveledCompactionStrategy', " + //
                //4个公共选项在AbstractCompactionStrategy中定义并由AbstractCompactionStrategy验证
                "'tombstone_threshold' : 0.2, 'tombstone_compaction_interval' : 86400, " + //
                "'unchecked_tombstone_compaction' : 'false', 'enabled' : 'true', " + //
                //注意这两选项没有被忽略，不能出现
                //"'min_threshold' : 6, 'max_threshold' : 16, " + //
                //SizeTieredCompactionStrategy专属选项，被忽略
                "'min_sstable_size' : 52428800, 'bucket_low' : 0.5, 'bucket_high' : 1.5, 'cold_reads_to_omit' : 0.05, " + //
                //LeveledCompactionStrategy专属选项
                "'sstable_size_in_mb' : 1}";
        tryExecute();
    }

    void insert() throws Exception {
        int count = 5;
        for (int i = 0; i < count; i++) {
            cql = "INSERT INTO " + tableName + "(id, f1, f2) " + //
                    "VALUES (" + i + ", " + i + ", 'T" + i + "') USING TTL 300";
            SimpleStatement stmt = new SimpleStatement(cql);
            stmt.setConsistencyLevel(ConsistencyLevel.TWO);
            stmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
            stmt.setConsistencyLevel(ConsistencyLevel.ONE);
            execute(stmt);
        }

        cql = "DELETE FROM " + tableName + " WHERE id=1";
        execute();

        cql = "DELETE f2 FROM " + tableName + " WHERE id=2 AND f1=2";
        execute();
        cql = "DELETE FROM " + tableName + " WHERE id=3 AND f1=3";
        execute();
    }

    void select() {
        cql = "select * from " + tableName + " where id in (1,2,3)";
        ResultSet rs = session.execute(cql);
        //rs.all();
        for (Row row : rs)
            System.out.println(row);
    }

}
