package com.rationalenterprise.mediadiff.service;

import org.apache.commons.text.StringTokenizer;
import picocli.CommandLine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "loadFile", description = "Diff two load files")
public class LoadFileService implements Callable<Integer> {
    public static final char UTF_8_BOM = '\uFEFF';

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-f1"}, description = "The path to a load file.")
    private Path file1;

    @CommandLine.Option(names = {"-f2"}, description = "The path to a load file.")
    private Path file2;

    @CommandLine.Option(names = "-datToJson", description = "Compare a dat and json.")
    boolean datToJson;

    /**
     * Diff two load files.
     *
     * @return
     * @throws Exception
     */
    @Override
    public Integer call() throws IOException {
        if (!file1.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: -f1 does not exist", file1.toString()));
        }

        if (!file2.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: -f2 does not exist", file2.toString()));
        }

        if (datToJson) {
            checkMD5Only();
        } else {
            compareDatFiles();
        }

        return 0;
    }

    private void checkMD5Only() throws IOException {
    }

    private void compareDatFiles() throws IOException {
        boolean passed = true;

        /*
        row counts
        compare headers
        compare the values for each document
            But there aren't document ids in common.  I'll need some other way of matching the rows in the 2 files.
                maybe the hash and the paths?
         */

        List<String> header1;

        try (BufferedReader br = new BufferedReader(new FileReader(file1.toFile(), StandardCharsets.UTF_8))) {
            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            // trim/read the header
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            header1 = Arrays.asList(t.reset(row).getTokenArray());

            System.out.println("file1 headers: " + header1.size());

            int rowCount = 0;

            while ((row = br.readLine()) != null) {
                rowCount++;
                List<String> values = Arrays.asList(t.reset(row).getTokenArray());

                if (header1.size() != values.size()) {
                    System.out.println("\nRow column count does not match header count for row: " + rowCount);
                }
            }
        }
        List<String> header2;
        try (BufferedReader br = new BufferedReader(new FileReader(file2.toFile(), StandardCharsets.UTF_8))) {
            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            // trim/read the header
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            header2 = Arrays.asList(t.reset(row).getTokenArray());
            System.out.println("\nfile2 headers: " + header2.size());

            int rowCount = 0;

            while ((row = br.readLine()) != null) {
                rowCount++;
                List<String> values = Arrays.asList(t.reset(row).getTokenArray());

                if (header2.size() != values.size()) {
                    System.out.println("\nRow column count does not match header count for row: " + rowCount);
                }
            }
        }

        List<String> header1MinusHeader2 = new ArrayList<>(header1);
        header1MinusHeader2.removeAll(header2);

        if (!header1MinusHeader2.isEmpty()) {
            String missing = header1MinusHeader2.stream().collect(Collectors.joining("\n"));

            System.out.println("\nExist in -f1 and missing in -f2:\n" + missing);
        }

        List<String> header2MinusHeader1 = new ArrayList<>(header2);
        header2MinusHeader1.removeAll(header1);

        if (!header2MinusHeader1.isEmpty()) {
            String missing = header2MinusHeader1.stream().collect(Collectors.joining("\n"));

            System.out.println("\nExist in -f2 and missing in -f1:\n" + missing);
        }

        if (passed) {
            System.out.println("\nAll tests passed.");
        } else {
            System.out.println("\nTests complete.");
        }
    }
}
