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
    private boolean compareHashes;

    @CommandLine.Option(names = "-datToJson", description = "Compare a dat and json.")
    private boolean datToJson;

    @CommandLine.Option(names = "-i", description = "Display value counts for all headers.  Compare the headers found in both load files.")
    private boolean inventory;

    @CommandLine.Option(names = "-ic", description = "Compare value counts for a column found in both load files.")
    private boolean inventoryColumn;

    @CommandLine.Option(names = "-v", description = "Find rows with values for header -h.")
    private boolean findValues;

    @CommandLine.Option(names = "-nv", description = "Find rows without values for header -h.")
    private boolean findRowsWithoutValues;

    @CommandLine.Option(names = "-h", description = "Header name.")
    private String headerName;

    @CommandLine.Option(names = "-chv", description = "Compare has value by hashes.")
    private boolean compareHasValueByHashes;

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

        if (!(inventory || inventoryColumn || findValues) && !file2.toFile().exists()) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("Invalid option: -f2 does not exist", file2.toString()));
        }

        if (findValues && (headerName == null ||  headerName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("-h must be a header name", file2.toString()));
        }

        if (findRowsWithoutValues && (headerName == null ||  headerName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("-h must be a header name", file2.toString()));
        }

        if (compareHasValueByHashes && (headerName == null ||  headerName.isBlank())) {
            throw new CommandLine.ParameterException(spec.commandLine(), String.format("-h must be a header name", file2.toString()));
        }

        if (inventory) {
            inventory();
        } else if (inventoryColumn) {
            inventoryColumn();
        } else if (compareHashes) {
            compareMD5SUMs();
        } else if (compareHasValueByHashes) {
            compareHasValueByHashes();
        } else if (findValues) {
            countRowsWithValue();
        } else if (findRowsWithoutValues) {
            countRowsWithoutValue();
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
        compareInventoryCounts(getHeaderToCountMap(file1), getHeaderToCountMap(file2));

        System.out.println("Test complete.");
    }

    private void inventoryColumn() throws IOException {
        compareInventoryCounts(getValueToCountMap(file1, headerName), getValueToCountMap(file2, headerName));

        System.out.println("Test complete.");
    }

    private void compareInventoryCounts(LinkedHashMap<String, Integer> f1ValuesToCount, LinkedHashMap<String, Integer> f2ValuesToCount) {
        List<String> f1MinusF2 = new ArrayList<>(f1ValuesToCount.keySet());
        f1MinusF2.removeAll(f2ValuesToCount.keySet());

        List<String> f2MinusF1 = new ArrayList<>(f2ValuesToCount.keySet());
        f2MinusF1.removeAll(f1ValuesToCount.keySet());

        if (!f1MinusF2.isEmpty()) {
            System.out.println(String.format("\nFound only in -f1 (%s):", f1MinusF2.size()));

            for (String f1Only : f1MinusF2) {
                System.out.println(String.format("(%s) %s\n", f1ValuesToCount.get(f1Only), f1Only));
            }
        }

        if (!f2MinusF1.isEmpty()) {
            System.out.println(String.format("\nFound only in -f2 (%s):", f2MinusF1.size()));

            for (String f2Only : f2MinusF1) {
                System.out.println(String.format("(%s) %s\n", f2ValuesToCount.get(f2Only), f2Only));
            }
        }

        Set<String> intersection = new HashSet<>();
        intersection.addAll(f1ValuesToCount.keySet());
        intersection.addAll(f2ValuesToCount.keySet());

        intersection.removeAll(f1MinusF2);
        intersection.removeAll(f2MinusF1);

        if (!(f1MinusF2.size() == f1ValuesToCount.size() && f2MinusF1.size() == f2ValuesToCount.size())) {
            System.out.println(String.format("\nIntersection comparison (%s):", intersection.size()));
            boolean intersectionDifference = false;

            for (Map.Entry<String, Integer> entry : f1ValuesToCount.entrySet()) {
                if (f2ValuesToCount.containsKey(entry.getKey())) {
                    if (!f1ValuesToCount.get(entry.getKey()).equals(f2ValuesToCount.get(entry.getKey()))) {
                        intersectionDifference = true;

                        System.out.println(String.format("-f1 (%s) %s, -f2 (%s) %s",
                                f1ValuesToCount.get(entry.getKey()), entry.getKey(), f2ValuesToCount.get(entry.getKey()), entry.getKey()));
                    }
                }
            }

            if (!intersectionDifference) {
                System.out.println("Intersections match.");
            }
        }
    }

    private LinkedHashMap<String, Integer> getValueToCountMap(Path metadataPath, String headerName) throws IOException {
        LinkedHashMap<String, Integer> valueToCount = new LinkedHashMap<>();

        try (BufferedReader br = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]).setIgnoreEmptyTokens(false);

            String headerString = row; // delete
            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            if (Collections.frequency(header, headerName) != 1) {
            }

            int columnIndex = header.lastIndexOf(headerName);

            while ((row = br.readLine()) != null) {
                String[] values = t.reset(row).getTokenArray();

                if (values[columnIndex].contains("?")) { // delete
                    System.out.println(headerString + "\n" + row);
                    break;
                }
                valueToCount.put(values[columnIndex], valueToCount.getOrDefault(values[columnIndex], 0) + 1);
            }
        }

        return valueToCount;
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
                        } else if (header.get(i).equals("RAWSIZE") && values[i].isBlank()) {
                            System.out.println(row);
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
    }

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

    public void countRowsWithValue() throws IOException {
        LinkedHashMap<String, Integer> headerToCount = new LinkedHashMap<>();

        try (BufferedReader br = Files.newBufferedReader(file1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            /*
            Collections.frequency(list, key));
             */
            int headerIndex = header.indexOf(headerName);
            List<String> rows = new ArrayList<>();
            int count = 0;

            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                if (header.size() != values.length) {
                    // throw an exception
                    System.out.println(String.format("Row #%s column count does not match header: %s row columns, %s header columns", count, values.length, header.size()));
                } else {
                    if (!values[headerIndex].isBlank()) {
                        rows.add(row);
                    }
                }
            }

            if (!rows.isEmpty()) {
                System.out.println(String.format("%s rows found with values for header %s", rows.size(), headerName));
//                System.out.println(rows.stream().collect(Collectors.joining("\n\n")));
            }
        }
    }

    public void countRowsWithoutValue() throws IOException {
        LinkedHashMap<String, Integer> headerToCount = new LinkedHashMap<>();

        try (BufferedReader br = Files.newBufferedReader(file1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            /*
            Collections.frequency(list, key));
             */
            int headerIndex = header.indexOf(headerName);
            List<String> rows = new ArrayList<>();
            int count = 0;

            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                if (header.size() != values.length) {
                    // throw an exception
                    System.out.println(String.format("Row #%s column count does not match header: %s row columns, %s header columns", count, values.length, header.size()));
                } else {
                    if (values[headerIndex].isBlank()) {
                        rows.add(row);
                    }
                }
            }

            System.out.println(String.format("%s rows found. %s have no value.", count, rows.size()));

            if (!rows.isEmpty()) {
                System.out.println(String.format("%s rows found without values for header %s", rows.size(), headerName));
                System.out.println(rows.stream().collect(Collectors.joining("\n\n")));
            }
        }
    }

    private void compareHasValueByHashes() throws IOException {
        Set<String> f1HasValue = new HashSet<>();
        Set<String> f1HasNoValue = new HashSet<>();

        try (BufferedReader br = Files.newBufferedReader(file1, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            int headerIndex = header.indexOf(headerName);
            int MD5Index = header.indexOf("MD5SUM");

            int count = 0;

            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                if (header.size() != values.length) {
                    // throw an exception
                    System.out.println(String.format("Row #%s column count does not match header: %s row columns, %s header columns", count, values.length, header.size()));
                } else {
                    if (!values[headerIndex].isBlank()) {
                        f1HasValue.add(values[MD5Index]);
                    } else {
                        f1HasNoValue.add(values[MD5Index]);
                    }
                }
            }

            for (String hash : f1HasValue) {
                if (f1HasNoValue.contains(hash)) {
                    System.out.println(String.format("Warning: %s is was found in the has values set and the has no values set.", hash));
                }
            }
        }

        Set<String> f2HasValue = new HashSet<>();
        Set<String> f2HasNoValue = new HashSet<>();
        try (BufferedReader br = Files.newBufferedReader(file2, StandardCharsets.UTF_8)) {
            String row = br.readLine();

            if (row.charAt(0) == UTF_8_BOM) {
                row = row.substring(1);
            }

            StringTokenizer t = new StringTokenizer("", Character.toChars(20)[0], Character.toChars(254)[0]);
            t.setIgnoreEmptyTokens(false);

            List<String> header = Arrays.asList(t.reset(row).getTokenArray());

            int headerIndex = header.indexOf(headerName);
            int MD5Index = header.indexOf("MD5SUM");

            int count = 0;

            while ((row = br.readLine()) != null) {
                count++;

                String[] values = t.reset(row).getTokenArray();

                if (header.size() != values.length) {
                    // throw an exception
                    System.out.println(String.format("Row #%s column count does not match header: %s row columns, %s header columns", count, values.length, header.size()));
                } else {
                    if (!values[headerIndex].isBlank()) {
                        f2HasValue.add(values[MD5Index]);
                    } else {
                        f2HasNoValue.add(values[MD5Index]);
                    }
                }
            }

            for (String hash : f2HasValue) {
                if (f2HasNoValue.contains(hash)) {
                    System.out.println(String.format("Warning: %s is was found in the has values set and the has no values set.", hash));
                }
            }
        }

        Set<String> f1HasValueMinusf2HasValue = new HashSet<>(f1HasValue);
        f1HasValueMinusf2HasValue.removeAll(f2HasValue);

        System.out.println(String.format("Has value in -f1 and not in -f2 (%s):\n%s", f1HasValueMinusf2HasValue.size(), f1HasValueMinusf2HasValue.stream().collect(Collectors.joining("\n"))));

        Set<String> f2HasValueMinusf1HasValue = new HashSet<>(f2HasValue);
        f2HasValueMinusf1HasValue.removeAll(f1HasValue);

        System.out.println(String.format("Has value in -f2 and not in -f1 (%s):\n%s", f2HasValueMinusf1HasValue.size(), f2HasValueMinusf1HasValue.stream().collect(Collectors.joining("\n"))));
    }
}
