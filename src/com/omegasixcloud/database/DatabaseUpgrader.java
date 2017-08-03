package com.omegasixcloud.catalogservice.common;

import com.google.common.io.Resources;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by amcde on 6/8/2017.
 */
public class DatabaseUpgrader {

    public static void upgradeDatabase(List<String> paths, Connection conn) throws Exception {

        List<String> failures = new ArrayList<>();

        try
        {
            for (String path : paths) {

                try {
                    // try upgrade as a file
                    File resFile = new File(Resources.getResource(path).getFile());
                    failures.addAll(upgradeDatabase(resFile.getAbsolutePath(), conn));
                } catch (IllegalArgumentException|NoSuchFileException e) {

                    try {
                        failures.addAll(upgradeDatabase(path, conn));
                    } catch (IllegalArgumentException|NoSuchFileException ex)
                    {
                        continue;
                    }
                } catch (Exception ex) {
                    failures.add(ex.getMessage());
                }
            }
        } finally {

            if(failures.stream().findAny().isPresent())
            {
                failures.stream().forEach(System.out::println);
                conn.rollback();
                conn.close();
                throw new Exception(String.join("\n", failures));
            } else
            {
                conn.commit();
            }

            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println("error caught: " + ex.getMessage());
            }
        }
    }

    private static List<String> upgradeDatabase(String sqlFileDirectory, Connection conn) throws Exception
    {
        createUpgradeTableIfNotExists(conn);
        List<String> failures = new ArrayList<>();
        try(Stream<Path> pathStream = Files.walk(Paths.get(sqlFileDirectory)).filter(p -> p.getFileName().toString().endsWith(".sql"))) {

            pathStream.forEach(p -> {
                try {
                    System.out.println("Processing " + p.toString());
                    applyUpgradeFromFile(p, conn);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    failures.add(e.getMessage());
                }
            });

            return failures;
        }
    }

    private static void applyUpgradeFromFile(Path path, Connection conn) throws Exception
    {
        String fileName = path.getFileName().toString();
        if(hasSqlFileBeenApplied(fileName, conn))
        {
            System.out.println("SQL file has already been run: " + fileName);
            return;
        }
        String allLines = Files.readAllLines(path).stream().filter(l -> !l.startsWith("--")).collect(Collectors.joining());
        String[] sqls = allLines.split(";");
        String currentSql = null;

        try
        {
            for(String sql : sqls)
            {
                currentSql = sql;
                System.out.println("Running SQL:\n" + currentSql);
                PreparedStatement ps = conn.prepareStatement(currentSql);
                ps.execute();
            }
            currentSql = null;
            recordCompletedSqlFile(fileName, conn);
        } catch (Exception e)
        {
            if(currentSql != null)
            {
                System.out.println("Failed on :\n" + currentSql);
            }
            System.out.println("error caught: " + e.getMessage());
            throw e;
        }
    }

    private static boolean hasSqlFileBeenApplied(String fileName, Connection conn) throws Exception
    {
        String getSql = "SELECT * FROM completed_upgrade_sql_files WHERE FileName=?";
        PreparedStatement ps = conn.prepareStatement(getSql);
        ps.setString(1, fileName);
        ResultSet rs = ps.executeQuery();

        if(rs.isBeforeFirst())
        {
            return true;
        }

        return false;
    }

    private static void recordCompletedSqlFile(String fileName, Connection conn) throws Exception
    {
        System.out.println("Recording successfully processed sql file: " + fileName);
        String recordSql = "INSERT INTO completed_upgrade_sql_files (FileName) VALUES (?);";
        PreparedStatement ps = conn.prepareStatement(recordSql);
        ps.setString(1, fileName);
        ps.execute();
        // commit outside this method
    }

    private static void createUpgradeTableIfNotExists(Connection conn) throws Exception
    {
        System.out.println("Creating upgrade table...");
        String upgradeTableSql =
                "CREATE TABLE IF NOT EXISTS completed_upgrade_sql_files" +
                        " (" +
                        " CompletedFilesKey BIGINT PRIMARY KEY AUTO_INCREMENT," +
                        " FileName VARCHAR(2000) NOT NULL," +
                        " DateApplied DATETIME DEFAULT CURRENT_TIMESTAMP" +
                        ");";
        PreparedStatement ps = conn.prepareStatement(upgradeTableSql);
        ps.executeUpdate();
        conn.commit();
    }
}

