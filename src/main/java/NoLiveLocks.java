import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Standalone variant using the original blocking_data table and PostgreSQL connection. */
public final class NoLiveLocks {
    private NoLiveLocks() {}
    static final int ROWS = 50000;
    static final double MAX_VALUE = 50.0;
    static final long SEED = 123456;
    static final int THREADS = 4; // Original professor template explicitly requires four
    static final String UPDATE = "UPDATE blocking_data SET data_value=?, modified_by=? WHERE data_value>=? AND data_value<=?";

    static class Task {
        final int id;
        final double[] starts, stops, values;
        Task(int id, int count, Random random) {
            this.id=id;
            starts=new double[count]; stops=new double[count]; values=new double[count];
            for (int i=0;i<count;i++) {
                stops[i]=random.nextDouble()*MAX_VALUE;
                starts[i]=random.nextDouble()*stops[i];
                values[i]=(float)(random.nextDouble()*MAX_VALUE);
            }
        }
    }
    static void setup() throws SQLException {
        try (Connection conn=ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
             Statement statement=conn.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS blocking_data");
            statement.executeUpdate("CREATE TABLE blocking_data (data_id integer primary key, data_value float, modified_by integer)");
            conn.setAutoCommit(false);
            try (PreparedStatement ps=conn.prepareStatement("INSERT INTO blocking_data VALUES (?,?,?)")) {
                Random random=new Random(SEED);
                for (int i=0;i<ROWS;i++) {
                    ps.setInt(1,i); ps.setDouble(2,random.nextDouble()*MAX_VALUE); ps.setInt(3,0);
                    ps.addBatch();
                    if (i%1000==999) ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) { conn.rollback(); throw e; }
        }
    }
    static List<Task> tasks(int taskCount, int queryCount) {
        Random random=new Random(SEED);
        List<Task> list=new ArrayList<>();
        for (int i=0;i<taskCount;i++) list.add(new Task(i,queryCount,random));
        return list;
    }
    static boolean retryable(SQLException e) {
        for (SQLException x=e; x!=null; x=x.getNextException()) {
            if ("40P01".equals(x.getSQLState()) || "40001".equals(x.getSQLState())) return true;
        }
        return false;
    }
    static void oneAttempt(Task task) throws SQLException {
        try (Connection conn=ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try (PreparedStatement ps=conn.prepareStatement(UPDATE)) {
                for (int i=0;i<task.values.length;i++) {
                    ps.setDouble(1,task.values[i]);
                    ps.setInt(2,task.id);
                    ps.setDouble(3,task.starts[i]);
                    ps.setDouble(4,task.stops[i]);
                    ps.executeUpdate();
                }
                conn.commit();
            } catch (SQLException e) {
                try { conn.rollback(); } catch (SQLException rollbackError) { e.addSuppressed(rollbackError); }
                throw e;
            }
        }
    }
    public static void main(String[] args) throws Exception {
        boolean fair = true;
        int tasks= args.length>=1 ? Integer.parseInt(args[0]) : 30;
        int queries= args.length>=2 ? Integer.parseInt(args[1]) : 3;
        if (tasks<1 || queries<1) throw new IllegalArgumentException("Tasks and queries must be positive");
        setup();
        List<Task> work=tasks(tasks,queries);
        Semaphore gate=new Semaphore(1,true);
        AtomicInteger finished=new AtomicInteger();
        AtomicLong retries=new AtomicLong();
        ExecutorService pool=Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures=new ArrayList<>();
        long start=System.nanoTime();
        for(Task task:work) {
            futures.add(pool.submit(() -> {
                boolean acquired=false;
                try {
                    if (fair) {gate.acquire(); acquired=true;}
                    int attempt=0;
                    while(true) {
                        attempt++;
                        try {
                            oneAttempt(task);
                            int count=finished.incrementAndGet();
                            System.out.printf("Task %d COMMIT (attempt %d); finished %d%n", task.id,attempt,count);
                            return;
                        } catch(SQLException e) {
                            if (!retryable(e)) throw new IllegalStateException("Non-retryable SQL failure in task "+task.id,e);
                            long count=retries.incrementAndGet();
                            System.out.printf("Task %d deadlock/serialization failure, retry %d (SQLSTATE=%s, total retries=%d)%n",task.id,attempt,e.getSQLState(),count);
                            // In RETRY mode, a small randomized backoff reduces simultaneous retries.
                            if(!fair) Thread.sleep(ThreadLocalRandom.current().nextLong(10, 61));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted task "+task.id,e);
                } finally {
                    if(acquired) gate.release();
                }
            }));
        }
        pool.shutdown();
        boolean error=false;
        for(Future<?> future:futures) {
            try {future.get();}
            catch(ExecutionException e) {error=true; System.err.println("TASK FAILED: "+e.getCause());}
        }
        if (!pool.awaitTermination(1,TimeUnit.MINUTES)) throw new IllegalStateException("Thread pool did not terminate");
        double elapsed=(System.nanoTime()-start)/1e9;
        System.out.printf("mode=%s tasks=%d queriesPerTask=%d threads=%d finished=%d retries=%d seconds=%.3f%n",fair?"FAIR":"RETRY",tasks,queries,THREADS,finished.get(),retries.get(),elapsed);
        if(error || finished.get()!=tasks) throw new IllegalStateException("Not all tasks completed");
    }
}
