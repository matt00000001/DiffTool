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

    @CommandLine.Option(names = "-ch", description = "Compare hashes.")
    boolean compareHashes;

    @CommandLine.Option(names = "-datToJson", description = "Compare a dat and json.")
    boolean datToJson;

    @CommandLine.Option(names = "-i", description = "Count rows.")
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
        } else if (compareHashes) {
            compareMD5SUMs();
        } else {
            compareDatFiles();
        }

        return 0;
    }

    /*
    - verify paths have corresponding files
    - verify columns have same values (column name argument)
    - verify rows match md5 comparison
    - find missing rows (include (has values) info)
     */

    private void inventory() throws IOException {
        /*
        get all the rows in f1 put them in a list
        put all the headers in f2 in a list
        get the differece of the list
            get the count of values for each different one
        get the count of values for the intersection

        display header count
        headers contained in both load files: in alphabetical order with value counts
        headers only existing in {-f1}
         */

        /*
        linked hash map with header names and counts
         */

        LinkedHashMap<String, Integer> f1HeaderToValuesCount = getHeaderToCountMap(file1);
        LinkedHashMap<String, Integer> f2HeaderToValuesCount = getHeaderToCountMap(file2);

        List<String> f1MinusF2 = new ArrayList<>(f1HeaderToValuesCount.keySet());
        f1MinusF2.removeAll(f2HeaderToValuesCount.keySet());

        List<String> f2MinusF1 = new ArrayList<>(f2HeaderToValuesCount.keySet());
        f2MinusF1.removeAll(f1HeaderToValuesCount.keySet());

        if (!f1MinusF2.isEmpty()) {
            System.out.println(String.format("\nFound only in -f1 (%s):", f1MinusF2.size()));

            for (String f1Only : f1MinusF2) {
                System.out.println(String.format("%s %s", f1Only, f1HeaderToValuesCount.get(f1Only)));
            }
        }

        if (!f2MinusF1.isEmpty()) {
            System.out.println(String.format("\nFound only in -f2 (%s):", f2MinusF1.size()));

            for (String f2Only : f2MinusF1) {
                System.out.println(String.format("%s %s", f2Only, f2HeaderToValuesCount.get(f2Only)));
            }
        }

        Set<String> intersection = new HashSet<>();
        intersection.addAll(f1HeaderToValuesCount.keySet());
        intersection.addAll(f2HeaderToValuesCount.keySet());

        intersection.removeAll(f1MinusF2);
        intersection.removeAll(f2MinusF1);

        if (!(f1MinusF2.size() == f1HeaderToValuesCount.size() && f2MinusF1.size() == f2HeaderToValuesCount.size())) {
            System.out.println(String.format("\nIntersection comparison (%s):", intersection.size()));

            for (Map.Entry<String, Integer> entry : f1HeaderToValuesCount.entrySet()) {
                if (f2HeaderToValuesCount.containsKey(entry.getKey())) {
                    System.out.println(String.format("-f1 %s %s, -f2 %s %s", entry.getKey(),
                            f1HeaderToValuesCount.get(entry.getKey()), entry.getKey(), f2HeaderToValuesCount.get(entry.getKey())));
                }
            }
        }

        System.out.println("Test complete.");
    }

    private LinkedHashMap<String, Integer> getHeaderToCountMap(Path metadataPath) throws IOException {
        LinkedHashMap<String, Integer> headerToCount = new LinkedHashMap<>();

        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            List<String> orderedHeaders = new ArrayList<>(header);

            orderedHeaders.sort((h1, h2) -> {
                int compareTo = h1.compareToIgnoreCase(h2);

                if (compareTo == 0) {
                    compareTo = h1.compareTo(h2);
                }

                return compareTo;
            });

            for (String header1 : orderedHeaders) {
                headerToCount.put(header1, 0);
            }

            int count = 0;
            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                if (header.size() != values.length) {
                    // throw an exception
                    System.out.println(String.format("Row #%s column count does not match header: %s row columns, %s header columns", count, values.length, header.size()));
                } else {
                    for (int i = 0; i < values.length; i++) {
                        if (!values[i].isBlank()) {
                            headerToCount.put(header.get(i), headerToCount.get(header.get(i)) + 1);
                        }
                    }
                }
            }
        }

        return headerToCount;
    }

    private void compareMD5SUMs() throws IOException {
        List<String> hashes1 = getHashes(file1);
        List<String> hashes2 = getHashes(file2);

        if (hashes1.size() != hashes2.size()) {
            System.out.println(String.format("%s hashes found in -f1 and %s hashes found in -f2", hashes1.size(), hashes2.size()));
        }

        List<String> hashes1MinusHashes2 = new ArrayList<>(hashes1);
        hashes1MinusHashes2.removeAll(hashes2);

        if (!hashes1MinusHashes2.isEmpty()) {
            System.out.println(String.format("Hashes found in -f1 only:\n", hashes1MinusHashes2.stream().collect(Collectors.joining("\n"))));
        }

        List<String> hashes2MinusHashes1 = new ArrayList<>(hashes2);
        hashes2MinusHashes1.removeAll(hashes1);

        if (!hashes2MinusHashes1.isEmpty()) {
            System.out.println(String.format("Hashes found in -f2 only:\n", hashes2MinusHashes1.stream().collect(Collectors.joining("\n"))));
        }

        // add order check
    }

    /*
    OI_ERROR

    there aren't unique hashes.  a native could be loaded n times so that hash will be in the file n times.
    the native paths should be the same but they are different

     */
    private List<String> getHashes(Path metadataPath) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            String[] header = t.reset(row).getTokenArray();

            int MD5Index;
            boolean MD5Found = false;

            for (MD5Index = 0; MD5Index < header.length; MD5Index++) {
                if (header[MD5Index].equals("MD5SUM")) {
                    MD5Found = true;

                    break;
                }
            }

            if (!MD5Found) {
                throw new CommandLine.ParameterException(spec.commandLine(), String.format("MD5SUM not found in %s", file1));
            }

            List<String> hashes = new ArrayList<>();

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                String hash = values[MD5Index];

                if (!isValidMD5(hash)) {
                    System.out.println(String.format("Invalid hash %s found in -%s", hash, metadataPath));
                }

                if (hashes.contains(hash)) {
                    System.out.println(String.format("%s exists more than once in %s", hash, metadataPath));
                }

                hashes.add(values[MD5Index]);
            }

            return hashes;
        }
    }

    public boolean isValidMD5(String s) {
        return s.matches("^[a-fA-F0-9]{32}$");
    }

    public boolean isValidSHA1(String s) {
        return s.matches("^[a-fA-F0-9]{40}$");
    }

    private void compareOrder() {

    }

    private void countF1Rows() throws IOException {

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
