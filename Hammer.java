/*
The MIT License (MIT)

Copyright (c) 2015 Miguel Barrientos

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package hammer;

//import static hammer.SimpleThreads.threadMessage;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 *
 * @author Miguel Barrientos
 */
public class Hammer {

    /**
     * @param args the command line arguments
     */
    public static void main(String args[])
            throws InterruptedException {

        // Delay, in milliseconds before we interrupt MessageLoop thread (default one hour).
        //long patience = 1000 * 60 * 60;
        int numThreads = 1;
        int numIterations = 1;

        // If command line argument present, gives patience in seconds.
        if (args.length > 0) {
            try {
                //patience = Long.parseLong(args[0]) * 1000;
                numThreads = Integer.parseInt(args[0]);
                numIterations = Integer.parseInt(args[1]);

                threadMessage("START: " + java.time.LocalDateTime.now().toString());
                //long startTime = System.currentTimeMillis();

                for (int i = 0; i < numIterations; i++) {

                    java.util.concurrent.ExecutorService es = java.util.concurrent.Executors.newCachedThreadPool();

                    for (int j = 0; j < numThreads; j++) {
                        es.execute(new MessageLoop());
                    }
                    es.shutdown();

                    try {
                        es.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
                        threadMessage("END ITERATION: " + i);
                    } catch (InterruptedException e) {
                    }

                    /*
                     for (int j = 0; j < numThreads; j++) {
                     Thread t = new Thread(new MessageLoop());
                     t.start();
                     }*/
                }
                threadMessage("END: " + java.time.LocalDateTime.now().toString());

            } catch (NumberFormatException e) {
                System.err.println("Invalid or insufficient number of arguments");
                System.exit(1);
            }
        }

    }

    // Display a message, preceded by
    // the name of the current thread
    static void threadMessage(String message) {
        String threadName = Thread.currentThread().getName();
        System.out.format("%s: %s%n", threadName, message);
    }

    private static class MessageLoop
            implements Runnable {

        public void run() {
            Statement stmt = null;
            ResultSet rs = null;
            Connection conn = null;

            try {
                Class.forName("cs.jdbc.driver.CompositeDriver");
                String url = "jdbc:compositesw:dbapi@SERVER_NAME:PORT_NUMBER?domain=DOMAIN_NAME&dataSource=DATABASE";

                conn = DriverManager.getConnection(url, USERNAME, PASSWORD);
                stmt = conn.createStatement();

                String sql = String.format("select distinct '%s' QUERY_ID, COLUMN_A FROM TABLE_A", Thread.currentThread().getName());
                if (stmt.execute(sql)) {
                    rs = stmt.getResultSet();

                    // Now do something with the ResultSet ....
                    while (rs.next()) {
                        //read value
                        String val = rs.getString(1);
                    }
                    threadMessage(java.time.LocalDateTime.now().toString());
                }
            } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                // it is a good idea to release
                // resources in a finally{} block
                // in reverse-order of their creation
                // if they are no-longer needed

                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException sqlEx) {
                    } // ignore

                    rs = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlEx) {
                    } // ignore

                    stmt = null;
                }
            }

        }
    }
}
