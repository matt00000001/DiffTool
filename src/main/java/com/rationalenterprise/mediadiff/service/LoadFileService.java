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

    @CommandLine.Option(names = "-inventory", description = "Count rows.")
    boolean inventory;

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

        if (!inventory && !file2.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: -f2 does not exist", file2.toString()));
        }

        if (inventory) {
            inventory();
        } else {
            compareDatFiles();
        }

        return 0;
    }

    private void inventory() throws IOException {

        try (BufferedReader br = Files.newBufferedReader(file1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            int rowCount = 0;
            while ((row = br.readLine()) != null) {
                rowCount++;

                if (rowCount % 500 == 0) {
                    System.out.print("\rRow: " + rowCount);
                }
            }

            System.out.print("\rRows: " + rowCount + "                                                    ");
        }
    }

    private void compareDatFiles() throws IOException {
        boolean passed = true;

        /*
        row counts
        compare headers
            if there are missing headers, do any of them have values?
        compare the values for each document
            But there aren't document ids in common.  I'll need some other way of matching the rows in the 2 files.
                maybe the hash and the paths? (hash and name).  But there are subdirectorires, so names aren't unique.
        Media mm made a second subdirectory around 10k files.
            to fix subdirectories, i need to go into the subdirectories and add those files to my list.  but the names are not guaranteed to be unique
            across subdirectories.
                for name comparison logic, if i get 2 of the same name the will both be in the list and when the loop that checks presence in other list
                will check the other list multiple times.

                if i put just the name in the list, i will not be able to access the file not knowing the subdirectory.  so i might need to put the
                name, path, and hash in a map.

                or i can report duplicate files and do nothing with them

                for the non name check, there will just be a check of the number of times the hashes occur.
                can the name check be changed to work based on hashes to fix the name issue?
                explain what the name issue is...
                    s1 contains a.txt s2 contains a.txt (a.txt is the same file)
                    s1 contains a.txt s2 contains a.txt (a.txt is not the same file)
                    name -> path -> hash
                    name -> hash -> path
                    hash -> path



         */

        List<String> header1;

        try (BufferedReader br = Files.newBufferedReader(file1, StandardCharsets.UTF_8)) {
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
