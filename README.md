# ADB Homework 2 — standalone classes

Based on the supplied professor Blocking.java. The two solution files contain their own logic and do not depend on HomeworkSupport.java.

## Files
- Blocking.java — baseline professor code adapted to compile with ConnectionFactory.java
- RestartBlocking.java — retries PostgreSQL SQLSTATE 40P01 / 40001 using fresh transactions and randomized backoff
- NoLiveLocks.java — fair Semaphore(1,true) admission within this JVM, retry if needed
- ConnectionFactory.java — local PostgreSQL connection adapter
- pom.xml — Maven PostgreSQL JDBC dependency

## Configuration
Set DB_URL=jdbc:postgresql://localhost:5432/adb_hw2, DB_USER=postgres, DB_PASSWORD=your password in IntelliJ Run Configurations.
Run Blocking without arguments, then RestartBlocking with `30 3`, NoLiveLocks with `30 3`. The revised classes use four worker threads and generate 50,000 rows. Run one at a time: each recreates blocking_data.

## Notes
These are illustrative student-work starting implementations based on the uploaded source, not verified against a running PostgreSQL database. The fair semaphore only coordinates tasks within this Java process, not external processes. No starvation proof across external clients is claimed. Preserve the original project's licensing and acknowledge outside code assistance according to class policy.
